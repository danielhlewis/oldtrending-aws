import os
import logging
import requests
import datetime
import json
import boto3
import botocore

bucket_name = os.environ.get('DATA_BUCKET')

"""From https://stackoverflow.com/questions/33842944/check-if-a-key-exists-in-a-bucket-in-s3-using-boto3"""
def checkS3ForFile(filename):
  s3_resource = boto3.resource('s3')
  try:
      s3_resource.Object(bucket_name, filename).load()
  except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == "404":
          # The object does not exist.
          return False
      else:
          # Something else has gone wrong.
          raise
  else:
      # The object exists.
      return True

def download_image(source, dest):
  if checkS3ForFile(dest):
    print('{} already exists in s3, skipping download')
    return False
  else:
    print("Downloading image: %s" % source)
    logging.info("(%s) Downloading image: %s" % (datetime.datetime.today(), source))
    # pull image from url
    print("Download Complete")
    r = requests.get(source, stream=True)
  # makes sure the request passed:
  if r.status_code == 200:
    s3 = boto3.client('s3')
    os.chdir('/tmp')
    tmp_name = os.path.basename(dest)
    print("Writing to {}".format(tmp_name))
    with open(tmp_name, 'wb') as f:
        f.write(r.content)
    print("Uploading to {}".format(dest))
    s3.upload_file(tmp_name, bucket_name, dest)
    return True
  else:
    print("Error: Could not download image: %s" % source)
    logging.error("(%s) Error downloading image: %s" % (datetime.datetime.today(), source))
    raise Exception

def handler(event, context):
  # target = event['target']
  # source_image = event['page']
  for record in event["Records"]:
    event_body = json.loads(record["body"])
    print('Event Body: {}'.format(event_body))
    message = json.loads(event_body['Message'])
    print('Message: {}'.format(message))
    target = message['target']
    source = message['source']
    dest = message['dest']
    info = message['info']
    bucket_dest = 'jp2/' + target+ '/' + dest
    print('Target Date: {}'.format(target))
    print('Source: {}'.format(source))
    print('Dest: {}'.format(dest))
    print('Info: {}'.format(info))
    print('Bucket Dest: {}'.format(bucket_dest))

    download_image(source, bucket_dest)

  # Eventually, we might want to publish to a topic whether this worked or not, but for now
  # since we are using s3 triggers to do the rest of the pipeline, we don't need to notify anybody
  return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/plain'
        },
        'body': 'Hello, I got an event: {}\n'.format(event)
    }

if __name__ == '__main__':
  bucket_name = "newspapercdkstack-newspaperdatapipeline098b5a34-17iv1rdoifwdc"
  test_message = {'Records': [{'MessageId': 'deaa3c86-0f71-4650-8ea6-53090d87ba33', 'ReceiptHandle': 'AQEB1YRhvwqqN6wl4jRfhWJyvJCZGQoktvlAb9v3qEf7P7yp7m3xhpNdr+yGHLnb88Mr0ggytDBCAVvCAUq+p/nAVYhGwPnbjltRfI8Qgj2AUZKfzonw5zxafyenFZ8EHYcYhmsDSMdrtFmaIKVkODueydG/U1urksvMlI2Z5150/2bpReSYEJCvwIP7sFi0Z5DUBqmOGvyg3C+QRd1F3NJLEzRQ7sLoAigEhKkipoemBh4HHyWkDk4r0fS3uVZmSO9oHj/7iRaUfjj7qWpGwV9DBQWUgzICdE2FgZcjM+Dw5EBlrFGeTNdP80E9f/2YjSnZx45fWqLq2ZvaEt6RE++Ocjo+yoQoRwJSPnumilXX8q6JlPJHEDYvxwi30b9507a0QoY/tvyQG8bnkMFMar8a6Q==', 'MD5OfBody': '48022afeca223cc0e737ac2b58b8b913', 'body': '{\n  "Type" : "Notification",\n  "MessageId" : "3dc590fb-ae87-5c08-b3bf-c968101552b5",\n  "TopicArn" : "arn:aws:sns:us-east-1:768752711595:test_get_pages_for_day",\n  "Message" : "{\\"target\\": \\"1921-06-01\\", \\"source\\": \\"https://chroniclingamerica.loc.gov/data/batches/ak_gyrfalcon_ver01/data/sn86072239/0027952651A/1921060101/1039.jp2\\", \\"dest\\": \\"1921060101-ak_gyrfalcon_ver01-sn86072239-0027952651A-1\\", \\"info\\": {\\"batch\\": \\"ak_gyrfalcon_ver01\\", \\"lccn\\": \\"sn86072239\\", \\"pub_date\\": \\"1921-06-01\\", \\"date_edition\\": \\"1921060101\\", \\"edition_seq_num\\": \\"01\\", \\"page_seq_num\\": 1, \\"subfolder\\": \\"0027952651A\\", \\"filename\\": \\"1039.jp2\\"}}",\n  "Timestamp" : "2021-08-16T01:16:02.170Z",\n  "SignatureVersion" : "1",\n  "Signature" : "CIGs6OwlPD6V4OX4qPmxmRq7SRjG5dmTjPrPILgojcebneqbqKUNF8FRZvsTLZNUmppmOXCJVYLFFA2rwTs0dcjLHRZZ6r+0NW9Vs74hnyRcenHlMBq0U0Pba0jHbfJx3pwLWZBrDk/KqCgkKKGnOdDOgUKnCd0EF6IPcvpaMMgMr2h+Mu/qPC65pAXxwcsqWRRDRnL7vZ1/OnVmYEvEUX/qiS7Z6OpBsRUNuta390vlSW0KMkpjcaESdX1fpw/qI+wMPrASS5e+/f2LeSDXGGPmaep6pIzw0HqdkaxEy/v6D9BF301GOtjcy1T730HQyjVO7Tjoa3oZwS+FzodaUw==",\n  "SigningCertURL" : "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-010a507c1833636cd94bdb98bd93083a.pem",\n  "UnsubscribeURL" : "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:768752711595:test_get_pages_for_day:907152b6-d294-4e7b-afa2-95d87d1cd651"\n}'}]}
  handler(test_message, None)
