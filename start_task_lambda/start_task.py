import os
import logging
import boto3

cluster_name = os.environ.get('CLUSTER_NAME')
container_id = os.environ.get('CONTAINER_ID')
task_arn = os.environ.get('TASK_ARN')
ecs = boto3.client('ecs')

def start_task(cluster_name: str, container_id: str, task_arn: str):
  response = ecs.start_task(cluster=cluster_name, 
                            containerInstances=[container_id], 
                            task_definition=task_arn)

def handler(event, context):
  print('Got Event: {}'.format(event))
  print('Environment Variables:')
  print('cluster_name: {}'.format(cluster_name))
  print('container_id {}'.format(container_id))
  print('task_arn {}'.format(task_arn))
  tasks_response = ecs.list_tasks(
    cluster=cluster_name
  )
  print('tasks_response: {}'.format(tasks_response))
  # If the task is already running, don't bother trying to start it again
  if task_arn not in tasks_response['taskArns']:
    print('Starting task')
    start_task(cluster_name, container_id, task_arn)
  else:
    print('task already running, skipping')

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
