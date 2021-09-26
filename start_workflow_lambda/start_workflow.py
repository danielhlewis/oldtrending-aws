import sys
import logging

import os
import boto3
import json
import datetime

lamda_name = os.environ.get('FIND_PAGES_LAMBDA')

_lambda = boto3.client('lambda')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("Run Started (logger)!")
print("Run Started (print)!")

def handler(event, context):
    dates = []
    for i in range(0,3):
      t = datetime.date.today() + datetime.timedelta(days=i)
      target = f'{t.year - 100:04}-{t.month:02}-{t.day:02}'
      dates.append(target)
      data = {'target': target}
      print(f'Starting Job for Target {target}')
      invoke_response = _lambda.invoke(FunctionName=lamda_name,
                                        InvocationType='Event',
                                        Payload=json.dumps(data))
      print(invoke_response)

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/plain'
        },
        'body': f'Hello, I have sent {dates}\n'
    }

if __name__ == "__main__":
  handler(None, None)