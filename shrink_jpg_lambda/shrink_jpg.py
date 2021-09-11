import os
import json
import boto3

bucket_name = os.environ['DATA_BUCKET']

def shrink_jpg(bucket_name, key):
    (_, date, filename) = key.split('/')
    basename = filename.split('.')[0]
    shrunken_name = basename + "-x720.jpg"
    upload_key = "banner/" + date + "/" + filename
    print('bucket_name: {}'.format(bucket_name))
    print('key: {}'.format(key))
    print('date: {}'.format(date))
    print('filename: {}'.format(filename))
    print('basename: {}'.format(basename))
    print('shrunken_name: {}'.format(shrunken_name))
    print('upload_key: {}'.format(upload_key))
    s3 = boto3.resource('s3')
    os.chdir('/tmp')
    s3.Bucket(bucket_name).download_file(key, filename)
    command = "/opt/bin/convert -resize x720 %s %s " % (filename, shrunken_name)
    os.system(command)
    s3.Bucket(bucket_name).upload_file(shrunken_name, upload_key)
    print("Success")

def handler(event, context):
  print('Got Event: {}'.format(event))
  # target = event['target']
  # source_image = event['page']
  sqs_body = json.loads(event["Records"][0]["body"])
  bucket_name = sqs_body['bucket_name']
  key = sqs_body['key']
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

  shrink_jpg(bucket_name, key)

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
          "key": "jp2/1921-06-01/1921060101-ak_gyrfalcon_ver01-sn86072239-0027952651A-1.jp2",
          "size": 3677289,
          "eTag": "02fa031892545685a31e1fdd08e9cd1b",
          "sequencer": "00611AD89F1A418AB8"
        }
      }
    }
  ]
}
  handler(test_message, None)
