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

        # Create an SNS Topic to publish our pages to
        pages_topic = sns.Topic(
          self, 'NewPagesTopic',
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

        pages_sqs = sqs.Queue(
          self, 'NewPagesQueue',
        )

        pages_topic.add_subscription(subs.SqsSubscription(pages_sqs))



