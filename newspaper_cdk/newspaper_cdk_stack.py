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
  aws_lambda_event_sources as sqs_sources,
  aws_s3 as s3
)

from aws_cdk.aws_lambda_python import PythonFunction

class NewspaperCdkStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # manifest_rds = rds.DatabaseInstance.from_database_instance_attributes(
        #   self, 'manifest_rds',
        #   instance_endpoint_address='newspaper-db.cx2rtxsijypk.us-east-1.rds.amazonaws.com',
        #   instance_identifier='newspaper-db',
        # )

        # # Find the existing VPC to add to
        # old_trending_vpc = ec2.Vpc.from_lookup(
        #   self, id='VPC', vpc_id='vpc-018e3a7a57b052b8c',
        #   )

        # Step 1: Check the database to find all of the pages to download
        pages_topic = self.buildGetPagesLambdaAndFriends()
        # Step 2: For each page, download it and convert from jp2 to jpg
        downloaded_topic = self.buildDownloadJP2AndFriends(pages_topic)



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
      )

      pages_topic.grant_publish(find_pages_lambda)
      return pages_topic

    def buildDownloadJP2AndFriends(self, input_topic):
        # Build input queue to subscribe to SNS topic for input
        pages_sqs = sqs.Queue(
          self, 'DownloadQueue',
        )
        input_topic.add_subscription(subs.SqsSubscription(pages_sqs))

        # We need imagemagick to convert jp2 files into jpeg files
        #  Since it isn't included by default, we need to build it as
        #  a layer.  It should already exist as a zip file (if it has
        #  been built from the submodule 'imagemagick-aws-lambda-2')
        #  so we just need to upload it
        imagemagick_layer = _lambda.LayerVersion(
          self, 'imagemagick',
          code=_lambda.AssetCode('imagemagick-aws-lambda-2/build/layer.zip'),
          compatible_runtimes=[_lambda.Runtime.PYTHON_3_8, 
                               _lambda.Runtime.PYTHON_3_7, 
                               _lambda.Runtime.PYTHON_3_6,
                               _lambda.Runtime.PYTHON_2_7,
                               _lambda.Runtime.NODEJS,]
        )

        # We will need an s3 bucket to download to, build it here
        dest_bucket = s3.Bucket(self, 'DownloadBucket')

        # We will publish the names of each file we download to an SNS topic
        output_topic = sns.Topic(
          self, 'DownloadedFiles',
        )

        # Now that we have the imagemagick layer and an output bucket,
        #  we can build the download lambda on top of it
        download_jp2_lambda = PythonFunction(
          self, 'DownloadJP2',
          entry='download_jp2_lambda',
          index='download_jp2.py',
          handler='handler',
          layers=[imagemagick_layer],
          environment= {
            'SNS_TOPIC_ARN': output_topic.topic_arn,
            'DESTINATION_BUCKET': dest_bucket.bucket_arn,
          },
        )

        # Give the lambda permission to write to the destination bucket
        dest_bucket.grant_read_write(download_jp2_lambda)
        output_topic.grant_publish(download_jp2_lambda)

        # Connect the lambda to the input sqs queue
        download_jp2_lambda.add_event_source(sqs_sources.SqsEventSource(pages_sqs))

        return {"Output_Bucket": dest_bucket,
                "Output_Topic": output_topic}