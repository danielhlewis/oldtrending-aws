from attr import attrs
from aws_cdk import core as cdk

# For consistency with other languages, `cdk` is the preferred import name for
# the CDK's core module.  The following line also imports it as `core` for use
# with examples from the CDK Developer's Guide, which are in the process of
# being updated to use `cdk`.  You may delete this import if you don't need it.
from aws_cdk import core

from aws_cdk import (
  aws_sns as sns,
  aws_lambda as _lambda,
  aws_sqs as sqs,
  aws_sns_subscriptions as subs,
  aws_lambda_event_sources as lambda_sources,
  aws_s3 as s3,
  aws_ecs as ecs,
  aws_ecr_assets as ecr_assets,
  aws_s3_notifications as s3_notifications,
  aws_ec2 as ec2,
  aws_iam as iam,
  aws_autoscaling as autoscaling,
  aws_logs as logs,
  aws_s3_deployment as s3_deployment,
)

from aws_cdk.aws_lambda_python import PythonFunction
from aws_cdk.aws_s3_assets import Asset

class NewspaperCdkStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # manifest_rds = rds.DatabaseInstance.from_database_instance_attributes(
        #   self, 'manifest_rds',
        #   instance_endpoint_address='newspaper-db.cx2rtxsijypk.us-east-1.rds.amazonaws.com',
        #   instance_identifier='newspaper-db',
        # )

        # Find the existing VPC to add to
        # TODO - make new VPC
        self.vpc = ec2.Vpc.from_lookup(
          self, id='VPC', vpc_id='vpc-f1e6358c',
        )

        # Setup: Create common storage. We will put data on it like so:
        # jp2/<date>/*.jp2
        # converted/<date>/*.jpg
        # converted/<date>/*-predictions.json
        # resized/<date>/*.jpg
        # resized/<date>/images.json
        self.data_bucket = s3.Bucket(self, 'NewspaperDataPipeline')
        self.web_bucket = s3.Bucket(self, 'NewspaperStaticSite', 
                                      public_read_access=True,
                                      website_index_document="spin.html",
                                      removal_policy=cdk.RemovalPolicy.DESTROY,
                                      auto_delete_objects=True)
        s3_deployment.BucketDeployment(self, 'Deployment', destination_bucket=self.web_bucket,
                                    sources=[s3_deployment.Source.asset("www")])

        # We need imagemagick to convert jp2 files into jpeg files
        #  Since it isn't included by default, we need to build it as
        #  a layer.  It should already exist as a zip file (if it has
        #  been built from the submodule 'imagemagick-aws-lambda-2')
        #  so we just need to upload it
        self.imagemagick_layer = _lambda.LayerVersion(
          self, 'imagemagick',
          code=_lambda.AssetCode('imagemagick-aws-lambda-2/build/layer.zip'),
          compatible_runtimes=[_lambda.Runtime.PYTHON_3_8, 
                               _lambda.Runtime.PYTHON_3_7, 
                               _lambda.Runtime.PYTHON_3_6,
                               _lambda.Runtime.PYTHON_2_7,
                               _lambda.Runtime.NODEJS,]
        )

        # Step 1: Check the database to find all of the pages to download
        pages_topic = self.buildGetPagesLambdaAndFriends()
        # Step 2: For each page, download it
        downloaded_topic = self.buildDownloadJP2AndFriends(pages_topic)
        # Step 3: Once the file is downloaded, convert from jp2 to jpg
        self.buildConvertJp2()
        # Step 5: Create Image for Detectron
        self.buildProcessPages()
        # Step 6: Check for banner headlines and resize files that have them
        shrink_queue = self.buildCheckHeadlines()
        # Step 7: Shrink Files that have banner headlines to a reasonable size
        self.buildShrinker(shrink_queue)


    def buildGetPagesLambdaAndFriends(self):
      # Create an SNS Topic to publish our pages to
      pages_topic = sns.Topic(
        self, 'NewPages',
      )

      find_pages_lambda = PythonFunction(
        self, 'FindPagesForDate',
        runtime=_lambda.Runtime.PYTHON_3_8,
        entry='get_pages_lambda',
        index='get_pages_for_day.py',
        handler='handler',
        # vpc=old_trending_vpc,
        # allow_public_subnet=True,
        environment= {
          'SNS_TOPIC_ARN': pages_topic.topic_arn,
        },
        timeout=core.Duration.seconds(10)
      )

      start_task_lambda = PythonFunction(
        self, 'StartWorkflow',
        runtime=_lambda.Runtime.PYTHON_3_8,
        entry='start_workflow_lambda',
        index='start_workflow.py',
        handler='handler',
        environment= {
          'FIND_PAGES_LAMBDA': find_pages_lambda.function_name,
        },
      )

      find_pages_lambda.grant_invoke(start_task_lambda)
      pages_topic.grant_publish(find_pages_lambda)
      return pages_topic

    def buildDownloadJP2AndFriends(self, input_topic):
        # Build input queue to subscribe to SNS topic for input
        pages_dead_sqs = sqs.Queue(
          self, 'DownloadQueueDeadLetter',
        )
        dlq = sqs.DeadLetterQueue(max_receive_count=4, queue=pages_dead_sqs)
        pages_sqs = sqs.Queue(
          self, 'DownloadQueue', dead_letter_queue=dlq,
        )
        input_topic.add_subscription(subs.SqsSubscription(pages_sqs))

        # We will publish the names of each file we download to an SNS topic
        output_topic = sns.Topic(
          self, 'DownloadedFiles',
        )

        download_jp2_lambda = PythonFunction(
          self, 'DownloadJP2',
          entry='download_jp2_lambda',
          index='download_jp2.py',
          handler='handler',
          environment= {
            'SNS_TOPIC_ARN': output_topic.topic_arn,
            'DATA_BUCKET': self.data_bucket.bucket_name,
          },
          timeout=core.Duration.seconds(10)
        )

        # Give the lambda permission to write to the destination bucket
        self.data_bucket.grant_read_write(download_jp2_lambda)
        output_topic.grant_publish(download_jp2_lambda)

        # Connect the lambda to the input sqs queue
        download_jp2_lambda.add_event_source(lambda_sources.SqsEventSource(pages_sqs))

        return output_topic

    def buildConvertJp2(self):
        # Now that we have the imagemagick layer, we can build the convert lambda on top of it
        convert_jp2_lambda = PythonFunction(
          self, 'ConvertJP2',
          entry='convert_jp2_lambda',
          index='convert_jp2.py',
          handler='handler',
          layers=[self.imagemagick_layer],
          environment= {
            'DATA_BUCKET': self.data_bucket.bucket_name,
          },
          timeout=core.Duration.minutes(1),
          memory_size=512,
        )

        # Give the lambda permission to write to the destination bucket
        self.data_bucket.grant_read_write(convert_jp2_lambda)
        
        # Connect the lambda to the s3 bucket
        convert_jp2_lambda.add_event_source(lambda_sources.S3EventSource(self.data_bucket,
          events=[s3.EventType.OBJECT_CREATED],
          filters=[s3.NotificationKeyFilter(prefix="jp2/", suffix="jp2")],
        ))

        return

    def buildProcessPages(self):
      jpg_sns_topic = sns.Topic(self, 'JpgTopic')
      self.data_bucket.add_event_notification(
                          s3.EventType.OBJECT_CREATED, 
                          s3_notifications.SnsDestination(jpg_sns_topic),
                          s3.NotificationKeyFilter(prefix="converted/", suffix="jpg"),
                          )

      jpg_sqs_queue = sqs.Queue(self, 'JpgQueue')
      jpg_sns_topic.add_subscription(subs.SqsSubscription(jpg_sqs_queue))

      detectron_image = ecr_assets.DockerImageAsset(self, "Detectron", directory="process_pages")
      print("{}".format(detectron_image))

      log_group = logs.LogGroup(self, "LogGroup")
      exec_bucket = s3.Bucket(self, "EcsLogs")

      cluster = ecs.Cluster(self, "DetectronCluster", 
                            vpc=self.vpc,
                            execute_command_configuration={
                              "log_configuration": {
                                  "cloud_watch_log_group": log_group,
                                  "s3_bucket": exec_bucket,
                                  "s3_key_prefix": "exec-command-output"
                              },
                              "logging": ecs.ExecuteCommandLogging.OVERRIDE
                            }
                            )
      # g4dn.xlarge seems to be the cheapest instance type with nvidia GPUs
      # t2.micro lets you stay in the free tier
      # t2.small is (one of) the cheapest with enough memory to hold the model
      # cluster.add_capacity("DefaultAutoScalingGroupCapacity",
      #     instance_type=ec2.InstanceType("t2.small"),
      #     min_capacity=0,
      #     desired_capacity=0,
      #     max_capacity=1,
      #     key_name='virginia',
      # )
      auto_scaling_group = autoscaling.AutoScalingGroup(self, "DetectronASG",
          vpc=self.vpc,
          instance_type=ec2.InstanceType("t2.small"),
          machine_image=ecs.EcsOptimizedImage.amazon_linux2(),
          min_capacity=0,
          max_capacity=1,
          instance_monitoring=autoscaling.Monitoring.BASIC,
          key_name='virginia',
          new_instances_protected_from_scale_in=False,
      )
      capacity_provider = ecs.AsgCapacityProvider(self, "AsgCapacityProvider",
          auto_scaling_group=auto_scaling_group,
          enable_managed_termination_protection=False,
      )
      cluster.add_asg_capacity_provider(capacity_provider)

      task_definition = ecs.Ec2TaskDefinition(self, "TaskDef")

      # container = task_definition.add_container("DefaultContainer",
      #     image=ecs.ContainerImage.from_registry(detectron_image.image_uri),
      #     environment= {
      #       "SQS_URL": "{}".format(jpg_sqs_queue.queue_url)
      #     },
      #     memory_limit_mib=512,
      # )
      container = task_definition.add_container("DefaultContainer",
          image=ecs.ContainerImage.from_asset("./process_pages"),
          environment= {
            "SQS_URL": "{}".format(jpg_sqs_queue.queue_url)
          },
          memory_limit_mib=1700,
          logging=ecs.LogDrivers.aws_logs(stream_prefix="detectron"),
      )

      start_task_lambda = PythonFunction(
        self, 'StartTask',
        runtime=_lambda.Runtime.PYTHON_3_8,
        entry='start_task_lambda',
        index='start_task.py',
        handler='handler',
        environment= {
          'CLUSTER_NAME': cluster.cluster_name,
          'CONTAINER_ID': task_definition.default_container.container_name,
          'TASK_ARN': task_definition.task_definition_arn,
        },
      )

      task_definition.execution_role.add_to_policy(iam.PolicyStatement(
        effect=iam.Effect.ALLOW,
        resources=['*'],
        actions=["sqs:DeleteMessage",
                "sqs:ListQueues",
                "sqs:GetQueueUrl",
                "sqs:ListDeadLetterSourceQueues",
                "sqs:DeleteMessageBatch",
                "sqs:ReceiveMessage",
                "sqs:GetQueueAttributes",
                "sqs:ListQueueTags"]
      ))

      task_definition.task_role.add_to_policy(iam.PolicyStatement(
        effect=iam.Effect.ALLOW,
        resources=['*'],
        actions=["sqs:DeleteMessage",
                "sqs:ListQueues",
                "sqs:GetQueueUrl",
                "sqs:ListDeadLetterSourceQueues",
                "sqs:DeleteMessageBatch",
                "sqs:ReceiveMessage",
                "sqs:GetQueueAttributes",
                "sqs:ListQueueTags",
                "s3:PutObject",
                "s3:GetObject",]
      ))

      # arn_parts = task_definition.task_definition_arn.split(':')
      # print(arn_parts)
      # Connect the lambda to the s3 bucket
      start_task_lambda.role.add_to_policy(iam.PolicyStatement(
        effect=iam.Effect.ALLOW,
        resources=['*'],
        actions=['ecs:ListTasks', 'ecs:RunTask', 'iam:PassRole'],
      ))
      start_task_lambda.add_event_source(lambda_sources.SnsEventSource(jpg_sns_topic))

    def buildCheckHeadlines(self):
        dlq_sqs = sqs.Queue(
          self, 'BannerHeadlinesDeadLetter',
        )
        dlq = sqs.DeadLetterQueue(max_receive_count=4, queue=dlq_sqs)
        banner_queue = sqs.Queue(
          self, 'BannerHeadlines', dead_letter_queue=dlq,
        )

        # Now that we have the imagemagick layer, we can build the convert lambda on top of it
        check_headlines_lambda = PythonFunction(
          self, 'CheckHeadlines',
          entry='check_headlines_lambda',
          index='check_headlines.py',
          handler='handler',
          environment= {
            'DATA_BUCKET': self.data_bucket.bucket_name,
            'BANNER_QUEUE_URL': banner_queue.queue_url,
          },
          timeout=core.Duration.seconds(10),
          memory_size=128,
        )

        self.data_bucket.grant_read_write(check_headlines_lambda)
        banner_queue.grant_send_messages(check_headlines_lambda)

        # Connect the lambda to the s3 bucket
        check_headlines_lambda.add_event_source(lambda_sources.S3EventSource(self.data_bucket,
          events=[s3.EventType.OBJECT_CREATED],
          filters=[s3.NotificationKeyFilter(prefix="converted/", suffix="-predictions.json")],
        ))

        return banner_queue

    def buildShrinker(self, shrink_queue):
        shrink_jpg_lambda = PythonFunction(
          self, 'ShrinkJpg',
          entry='shrink_jpg_lambda',
          index='shrink_jpg.py',
          handler='handler',
          layers=[self.imagemagick_layer],
          environment= {
            'DATA_BUCKET': self.data_bucket.bucket_name,
            'WEB_BUCKET': self.web_bucket.bucket_name,
          },
          timeout=core.Duration.seconds(30),
          memory_size=512,
        )

        # Give the lambda permission to write to the destination bucket
        self.data_bucket.grant_read(shrink_jpg_lambda)
        self.web_bucket.grant_read_write(shrink_jpg_lambda)

        shrink_jpg_lambda.add_event_source(lambda_sources.SqsEventSource(shrink_queue))

        return