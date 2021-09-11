import os
import json
import boto3

bucket_name = os.environ['DATA_BUCKET']
banner_queue_url = os.environ['BANNER_QUEUE_URL']

def read_predictions_file(bucket_name, key):
    filename = key.split('/')[-1]
    s3 = boto3.resource('s3')
    os.chdir('/tmp')
    s3.Bucket(bucket_name).download_file(key, filename)
    
    with open(filename) as fin:
      info = json.load(fin)
      if 'Headline' in info['predictions']:
        headlines = info["predictions"]['Headline']
        for headline in headlines:
          if headline['score'] < .7:
            continue
          box = headline['box']
          if ((box[2] - box[0]) / info['image_width'] >= .70):
            return True
    return False

def handler(event, context):
  print('Got Event: {}'.format(event))
  # target = event['target']
  # source_image = event['page']
  s3_info = event["Records"][0]["s3"]
  bucket_name = s3_info['bucket']['name']
  key = s3_info['object']['key']
  # print('Event Body: {}'.format(event_body))
  # message = json.loads(event_body['Message'])
  # print('Message: {}'.format(message))
  # target = message['target']
  # source = message['source']
  # dest = message['dest']
  # info = message['info']
  # print('Target Date: {}'.format(target))
  # print('Source: {}'.format(source))
  # print('Dest: {}'.format(dest))
  # print('Info: {}'.format(info))

  has_banner = read_predictions_file(bucket_name, key)
  if has_banner:
    print("{} contains banner headline!".format(key))
    sqs = boto3.client('sqs')
    data = {'bucket_name': bucket_name, 'key': key.replace('-predictions.json', '.jpg')}
    sqs.send_message(QueueUrl=banner_queue_url, MessageBody=json.dumps(data))
  else:
    print("{} does not contain banner headline!".format(key))

  return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/plain'
        },
        'body': 'Hello, I got an event: {}\n'.format(event)
    }

if __name__ == '__main__':
  test_message = {
  "Records": [
    {
      "eventVersion": "2.1",
      "eventSource": "aws:s3",
      "awsRegion": "us-east-1",
      "eventTime": "2021-08-16T21:29:00.336Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "AWS:AROA3F7J3O6VRZXXP44MX:NewspaperCdkStack-DownloadJP2F7A2304F-rJ4rn56jkEnI"
      },
      "requestParameters": {
        "sourceIPAddress": "3.94.249.43"
      },
      "responseElements": {
        "x-amz-request-id": "TA6PCVYYEWJDHKQ5",
        "x-amz-id-2": "Onqe3BSmMheY1tRrpXzVHuLXx2fANclPeldF2QcDxiKYy8MBG5GN26PZBnE62gjkixnm1VdYdgTshw20tJewY+95zy9xAlgrFa/7c/JN+9M="
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "ZWZlYTQwMzMtYmE1Zi00OGI0LWI3MTAtODI1MTQ4NjNmMjAy",
        "bucket": {
          "name": "newspapercdkstack-newspaperdatapipeline098b5a34-17iv1rdoifwdc",
          "ownerIdentity": {
            "principalId": "AGAYO2ZB4RXBE"
          },
          "arn": "arn:aws:s3:::newspapercdkstack-newspaperdatapipeline098b5a34-17iv1rdoifwdc"
        },
        "object": {
          "key": "jp2/1921-06-01/1921060101-ak_gyrfalcon_ver01-sn86072239-0027952651A-1-predictions.json",
          "size": 3677289,
          "eTag": "02fa031892545685a31e1fdd08e9cd1b",
          "sequencer": "00611AD89F1A418AB8"
        }
      }
    }
  ]
}
  handler(test_message, None)
