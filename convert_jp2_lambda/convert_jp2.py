import os
import logging
import boto3

bucket_name = os.environ['DATA_BUCKET']

def convert_image(bucket_name, key):
    (_, date, filename) = key.split('/')
    basename = filename.split('.')[0]
    jpg_name = basename + ".jpg"
    upload_key = "converted/" + date + "/" + jpg_name
    s3 = boto3.resource('s3')
    os.chdir('/tmp')
    s3.Bucket(bucket_name).download_file(key, filename)
    command = "/opt/bin/convert %s %s" % (filename, jpg_name)
    os.system(command)
    s3.Bucket(bucket_name).upload_file(jpg_name, upload_key)
    print("Success")

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

  convert_image(bucket_name, key)

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
