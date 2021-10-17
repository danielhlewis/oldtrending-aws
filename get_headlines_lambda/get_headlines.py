import sys
import logging
from typing import Protocol
from botocore.endpoint import Endpoint
import mysql.connector
from mysql.connector import errorcode

import rds_config
#rds settings
rds_host  = rds_config.db_host
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name

import os
import boto3
import botocore.exceptions
import json

topic_arn = os.environ.get('SNS_TOPIC_ARN')

sns = boto3.client('sns')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("Run Started (logger)!")
print("Run Started (print)!")

try:
  cnx = mysql.connector.connect(user=name, password=password, host=rds_host)
except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    logger.error("Something is wrong with your user name or password")
    logger.error(err)
  else:
    logger.error(err)
    sys.exit()
except Exception as err:
    logger.error(err)
    sys.exit()

cursor = cnx.cursor()
try:
  cursor.execute("USE {}".format(db_name))
except mysql.connector.Error as err:
  logger.error("Database {} does not exists.".format(db_name))
  logger.error(err)
  sys.exit()
except Exception as err:
  logger.error(err)
  sys.exit()

def getFrontPagesSQL(cursor, date):
  print([cursor, date])
  cursor.execute("""SELECT batch.name, image.lccn, image.pub_date, image.edition_seq_num, image.page_seq_num, image.subfolder, image.filename
                    FROM image JOIN batch ON batch.id = image.batch_id
                    WHERE pub_date = %s AND page_seq_num = 1""", (date,))
  return cursor.fetchall()

def getFrontPages(target):
  pages = getFrontPagesSQL(cursor, target)
  base_url = "https://chroniclingamerica.loc.gov/data/batches"
  requests = []
  output = {"target": target}
  for page in pages:
    (batch, lccn, pub_date, edition_seq_num, page_seq_num, subfolder, filename) = page
    # Date and edition number are combined into a single field in the source url
    date_edition = ''.join(pub_date.split('-')) + edition_seq_num
    # Build the parameters that we will pass forward in the message
    src = "%s/%s/data/%s/%s/%s/%s" % (base_url, batch, lccn, subfolder, date_edition, filename)
    # In order to ensure that we don't stomp on any other files, give each file a unique name from
    #  a combination of the info about it
    dest = "%s-%s-%s-%s-1.jp2" % (date_edition, batch, lccn, subfolder)
    info = {'batch': batch, 'lccn': lccn, 'pub_date': pub_date, 'date_edition': date_edition,
            'edition_seq_num': edition_seq_num, 'page_seq_num': page_seq_num, 
            'subfolder': subfolder, 'filename': filename}
    requests.append({'target': target, 'source':src, 'dest':dest, 'info': info})
  output['data'] = requests
  output['result'] = 'success'
  
  return output

def handler(event, context):
    # target = "1921-06-01"
    target = event['target']
    print("Starting job: target date=%s" % target)
    results = getFrontPages(target)
    print("Found {} pages".format(len(results['data'])))
    print("Publishing to SNS Topic ({})".format(topic_arn))
    for result in results['data']:
      sns_result = sns.publish(TopicArn=topic_arn, Message=json.dumps(result))
    print("All messages published to SNS")

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/plain'
        },
        'body': 'Hello, I have fetched {}\n'.format(len(results['data']))
    }

"""
Because CDK creates the topic, we don't know what the ARN will be.
If we are running locally and want to actually send messages to the topic on AWS,
we need to look it up.
"""
def find_topic_arn_from_local():
  # Find the correct sns topic if we are running locally
  next_token = ''
  while not topic_arn:
    response = sns.list_topics(NextToken=next_token)
    if 'Topics' in response:
      for topic in response['Topics']:
        if 'NewspaperCdkStack-NewPages' in topic['TopicArn']:
          topic_arn = topic['TopicArn']
          break
    if 'NextToken' in response:
      next_token = response['NextToken']
    else:
      break
  if not topic_arn:
    # If we haven't deployed the stack at least once, there will be no sns topic to publish to.
    # TODO generate a standalone topic so this can be tested in isolation rather than dropping messages into an actual topic
    print("No topic found, have you run 'cdk deploy' first?")
    os.exit()
  return topic_arn


if __name__ == "__main__":
  sqs = boto3.client('sqs')
  test_event = {"target": "1921-06-01"}
  # set isolate to true if we just want to test this script in isolation,
  # set to false to send the results into the real sns topic and let the subscribers
  # do their things
  isolate = True
  queue = None
  if isolate:
    # create_topic is idempotent, so we can just try the create and if the topic alread exists,
    #  it will give us the arn of the existing topic, otherwise it will create it
    topic_arn = sns.create_topic(Name='test_get_pages_for_day')['TopicArn']
    # sqs create, however, sucks so we have to check for existing queues first
    sqs_client = boto3.resource('sqs')
    try:
      queue = sqs_client.get_queue_by_name(QueueName='test_get_pages_for_day_queue')
      print("Found Existing Queue: {}".format(queue))
    except sqs_client.meta.client.exceptions.QueueDoesNotExist as error:
      # if the queue doesn't exist, make it
      queue = sqs_client.create_queue(QueueName='test_get_pages_for_day_queue')
      print("Created New Queue: {}".format(queue))
    except:
      print(sys.exc_info()[0])
      print("Undefined Exception")
      os.exit()
    # subscribe the queue to the topic
    queue_url = queue.url
    queue_arn = queue.attributes['QueueArn']
    print('{}'.format(queue_arn))

    policy = {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Sid": "topic-subscription-arn:{}".format(topic_arn),
          "Effect": "Allow",
          "Principal": {
            "AWS": "*"
          },
          "Action": "SQS:SendMessage",
          "Resource": "{}".format(queue_arn),
          "Condition": {
            "ArnLike": {
              "aws:SourceArn": "{}".format(topic_arn)
            }
          }
        }
      ]
    }
    sqs.set_queue_attributes(QueueUrl=queue_url, Attributes={'Policy': json.dumps(policy)})
    #Subscribe queue to topic (if we are already subscribed, no problem)
    sns.subscribe(TopicArn=topic_arn, Protocol='sqs', Endpoint=queue_arn)

  else:
    find_topic_arn_from_local()
  print("Will push results to the following topic: {}".format(topic_arn))

  results = getFrontPages(test_event['target'])
  print("Found {} pages".format(len(results['data'])))
  print("Publishing to SNS Topic ({})".format(topic_arn))
  sns_result = sns.publish(TopicArn=topic_arn, Message=json.dumps(results['data'][0]))
  print("One message published to SNS: {}".format(results['data'][0])) 

  if isolate:
    import time
    time.sleep(1)
    print("Checking for message in SQS...")
    response = sqs.receive_message(QueueUrl=queue.url, MaxNumberOfMessages=1)
    
    message = response['Messages'][0]
    body = json.loads(message['Body'])
    original = json.loads(body['Message'])
    print('{}'.format(message))
    print('{}'.format(original))
    if results['data'][0] == original:
      print("Pass!")
    else:
      print("Fail!")
    receipt_handle = message['ReceiptHandle']

    # Delete received message from queue
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )
