import os
import logging
import requests
import datetime
import json
import boto3

bucket_name = os.environ['DESTINATION_BUCKET']

def download_image(image_url, image_name):
  if os.path.exists(image_name):
    if (os.stat(image_name).st_size == 0):
      print('Empty file exists at destination, deleting it to redownload')
      logging.info('Empty file exists at destination, deleting it to redownload')
      os.remove(image_name)
    else:
      print('Image already exists at destination: %s' % image_name)
      logging.info("(%s) Destination image already exists: %s" % (datetime.datetime.today(), image_name))
      return 'skip'
  print("Downloading image: %s" % image_url)
  logging.info("(%s) Downloading image: %s" % (datetime.datetime.today(), image_url))
  # pull image from url
  r = requests.get(image_url, stream=True)
  # makes sure the request passed:
  if r.status_code == 200:
    os.makedirs(os.path.dirname(image_name), exist_ok=True)
    jp2_name = image_name.replace('.jpg', '.jp2')
    with open(jp2_name, 'wb') as f:
        f.write(r.content)
    command = "convert %s %s" % (jp2_name, image_name)
    # command = "opj_decompress -i %s -o %s" % (jp2_name, image_name)
    os.system(command)
    print("Success")
    logging.info("(%s) Image Downloaded: %s" % (datetime.datetime.today(), image_url))
    return 'success'
  else:
    print("Error: Could not download image: %s" % image_url)
    logging.error("(%s) Error downloading image: %s" % (datetime.datetime.today(), image_url))
    raise Exception

def handler(event, context):
  # target = event['target']
  # source_image = event['page']
  event_body = json.loads(event["Records"][0]["body"])
  print('Event Body: {}'.format(event_body))
  message = json.loads(event_body['Message'])
  print('Message: {}'.format(message))
  target = message['target']
  source = message['source']
  dest = message['dest']
  info = message['info']
  print('Target Date: {}'.format(target))
  print('Source: {}'.format(source))
  print('Dest: {}'.format(dest))
  print('Info: {}'.format(dest))

  return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/plain'
        },
        'body': 'Hello, I got an event: {}\n'.format(event)
    }

if __name__ == '__main__':
  test_message = {'Records': [{'messageId': '8037cf8d-f89f-4831-a673-98a0f13320cb', 'receiptHandle': 'AQEBhTOHY7KUrUU+0aDg+DydHajS0F8Pnx0w6O+z8pUrdwuTvOtwx94+6CLOUJhRW2jzD8KvBYRtU6IjjKZpkk0xxjd1D0wWblJUZ/k/6vqUbxdRIuKJAdQS8BKPvflNyliYT2aQU+Y8kYO751cW0lRtptjU4FNZjS/dJR2VkW5DqnPVhHRVTSuhvHadLly5VaKLazsbTwowi1FUO8vyhzSHYB6da3h7YD4VbJ3Lb7BcGHFOhhN1OT2tgc1ooGXuR4h9fpcKeSD+kdujZ4m9HW3ojwrTNNeSh6IRxL8DrCqDJc7NKJjjRPZhU6MQirZ/aKXLBA79CHSfX4HWgDAIHwabCokjFBTpjAaYDQv6nuNkZJlFpZzFs0vPnSoOv3Pr7ucB0O6HV37p+uoLDjT85IkiEEaNk9iK0CUI0h4M+0MPUToAep45mffoCG6lpqlc9BjM', 'body': '{\n  "Type" : "Notification",\n  "MessageId" : "bce2470b-5924-58f0-953c-1e65d4dfb759",\n  "TopicArn" : "arn:aws:sns:us-east-1:768752711595:NewspaperCdkStack-NewPagesD9BE62A1-1BVO1R6829T5V",\n  "Message" : "{\\"target\\": \\"1921-06-01\\", \\"page\\": \\"https://chroniclingamerica.loc.gov/data/batches/ct_doors_ver01/data/sn82014519/00414219093/1921060101/0012.jp2\\"}",\n  "Timestamp" : "2021-07-18T20:14:51.374Z",\n  "SignatureVersion" : "1",\n  "Signature" : "efWARsoRbGs2Rg8ldkUf0kayYaOPMD5FMhlr0iQuGYFV8jjcfRLY8QoRrz5AWZDKIWZ9l3rNAwB3dvpZlCwbxU4mXPbPsLFdLLM8qTTrGK+HDCgw8kG7qHwilVm/Wnv6af7n7f81vglVgxwrVjXnb0kcAE+hNBUiEXKcCEgxhYORKsOy8rFpQUQlxVPfXvTvTxY7QhSRKqkKQ+VUEMHA2BC0E3XhH7NW7aryJGx4cBot2DzB35t7YWUYC7p7aF/D5MY1pGwGhLMu7aj8WcShKj91uogIVPZcap3HfdPMzXSh+qQkFvwd1EiIDj2MM9Rz2i55lM5qSLCPcMalQbndEw==",\n  "SigningCertURL" : "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-010a507c1833636cd94bdb98bd93083a.pem",\n  "UnsubscribeURL" : "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:768752711595:NewspaperCdkStack-NewPagesD9BE62A1-1BVO1R6829T5V:c9cddc60-d4eb-4f84-b3cb-ec06661e59e0"\n}', 'attributes': {'ApproximateReceiveCount': '1', 'SentTimestamp': '1626639291400', 'SenderId': 'AIDAIT2UOQQY3AUEKVGXU', 'ApproximateFirstReceiveTimestamp': '1626639291401'}, 'messageAttributes': {}, 'md5OfBody': '437f44ad79f2c5e81bc8494b0c64e0ef', 'eventSource': 'aws:sqs', 'eventSourceARN': 'arn:aws:sqs:us-east-1:768752711595:NewspaperCdkStack-DownloadQueue0A824017-1R7XY4EBQ9D5U', 'awsRegion': 'us-east-1'}]}
  handler(test_message, None)
