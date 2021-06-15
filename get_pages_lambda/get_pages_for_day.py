import sys
import logging
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
import json

sns = boto3.client('sns')
topic_arn = os.environ['SNS_TOPIC_ARN']

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

def getFrontPages(cursor, date):
  print([cursor, date])
  cursor.execute("""SELECT batch.name, image.lccn, image.pub_date, image.edition_seq_num, image.page_seq_num, image.subfolder, image.filename
                    FROM image JOIN batch ON batch.id = image.batch_id
                    WHERE pub_date = %s AND page_seq_num = 1""", (date,))
  return cursor.fetchall()

def handler(event, context):
    target = "1921-06-01"
    print("Starting job: target date=%s" % target)
    pages = getFrontPages(cursor, target)
    base_url = "https://chroniclingamerica.loc.gov/data/batches"
    requests = []
    output = {"target": target}
    for page in pages:
      (batch, lccn, pub_date, edition_seq_num, page_seq_num, subfolder, filename) = page
      date_edition = ''.join(pub_date.split('-')) + edition_seq_num
      src = "%s/%s/data/%s/%s/%s/%s" % (base_url, batch, lccn, subfolder, date_edition, filename)
      dest = "/data/images/%s/%s-%s-%s-%s-1.jpg" % (target, date_edition, batch, lccn, subfolder)
      # addRequestToQueue(src, dest)
      requests.append({'source':src, 'dest':dest})
    output['data'] = requests
    output['result'] = 'success'

    # for page in output['data']:
    #   message = {'date': target, 'source': page['source']}
    #   response = sns.publish(
    #     TopicArn=topic_arn,
    #     Message=json.dumps(message),
    #   )

    print("Pushing to SNS Topic ({}): {}".format(topic_arn, output['data']))
    response = sns.publish(
      TopicArn=topic_arn,
      Message=json.dumps(output['data']),
    )
    print("Pushing to SNS completed")

    # print("%s results found for target %s" % (len(requests), target))
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/plain'
        },
        'body': 'Hello, I have fetched {}\n'.format(output['data'])
    }

if __name__ == "__main__":
  print([cnx, cursor])
  print(handler(None, None))