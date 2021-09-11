"""
Process the predownloaded newspaper pages to identify headlines, ads, etc.
"""
# import some common libraries
import cv2
import json
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches

# import deep learning imports
import detectron2
import torch
from detectron2.config import get_cfg
from detectron2.data import DatasetCatalog, MetadataCatalog
from detectron2.data.datasets import register_coco_instances
from detectron2.engine import DefaultTrainer
from detectron2.engine import DefaultPredictor
from detectron2.evaluation import COCOEvaluator
from detectron2.utils.visualizer import Visualizer
from detectron2.utils.visualizer import ColorMode
from detectron2.utils.logger import setup_logger
from detectron2.modeling import build_model
from detectron2.checkpoint import DetectionCheckpointer

#import libraries for microservice communication, logging, etc.
import logging
import json
import datetime
import os
import sys

import boto3

def build_model():
  setup_logger()
  cfg = get_cfg()
  cfg.merge_from_file("/home/appuser/process_pages/COCO-Detection/faster_rcnn_R_50_FPN_3x.yaml")
  # sets number of object classes to 7
  # ("Illustration/Photograph", "Photograph", "Comics/Cartoon", "Editorial Cartoon", "Map", "Headline", "Ad")
  cfg.MODEL.ROI_HEADS.NUM_CLASSES = 7
  cfg.MODEL.WEIGHTS = "/home/appuser/process_pages/model_final.pth"
  cfg.MODEL.DEVICE='cpu'
  
  return DefaultPredictor(cfg)


def writePredictionsFile(filename, predictions):
  categories = ["Photograph", "Illustration", "Map", "Comics/Cartoon", "Editorial Cartoon", "Headline", "Advertisement"]
  with open(filename, "w") as fout:
    boxes = predictions.get_fields()["pred_boxes"].tensor.tolist()
    scores = predictions.get_fields()["scores"].tolist()
    classes = predictions.get_fields()["pred_classes"].tolist()
    items = list(zip(boxes, scores, classes))
    stats = {}
    (image_height, image_width) = predictions.image_size
    stats["image_height"] = image_height
    stats["image_width"] = image_width
    stats["predictions"] = {}
    for category in range(0,7):
      members = list(filter(lambda member: (member[2] == category), items))
      if members:
        stats["predictions"][categories[category]] = []
        for member in members:
          prediction = {}
          prediction["box"] = member[0]
          prediction["score"] = member[1]
          stats["predictions"][categories[category]].append(prediction)
    json.dump(stats, fout)

def process_page(image, visualize_output=False):
  print("Processing page: {}".format(image))
  predictions_file = image.replace(".jpg", "-predictions.json")
  if os.path.exists(predictions_file):
    return ('skip', predictions_file)
  im = cv2.imread(image)
  outputs = predictor(im)
  detections = outputs['instances']
  writePredictionsFile(predictions_file, detections)
  
  #Do we want to visualize the output?
  if (visualize_output):
    confident_detections = detections[detections.scores > 0.9]
    headlines = confident_detections[confident_detections.pred_classes == 5]

    # if we want to save the images:
    v = Visualizer(im[:, :, ::-1],
                    scale=1.2   )
    v = v.draw_instance_predictions(headlines.to("cpu"))
    cv2.imwrite(image.replace('.jpg', '_out.jpg'), v.get_image()[:, :, ::-1])
  return ('success', predictions_file)

def create_sqs_queue_for_local(queue_name):
  # sqs create, however, sucks so we have to check for existing queues first
  sqs_client = boto3.resource('sqs')
  try:
    queue = sqs_client.get_queue_by_name(QueueName=queue_name)
    print("Found Existing Queue: {}".format(queue))
  except sqs_client.meta.client.exceptions.QueueDoesNotExist as error:
    # if the queue doesn't exist, make it
    queue = sqs_client.create_queue(QueueName=queue_name)
    print("Created New Queue: {}".format(queue))
  except:
    print(sys.exc_info()[0])
    print("Undefined Exception")
    os.exit()
  # subscribe the queue to the topic
  queue_url = queue.url
  queue_arn = queue.attributes['QueueArn']
  print('{}'.format(queue_arn))
  return queue_url

def build_test_message():
  return {
    "Records": [
      {
        "eventVersion": "2.1",
        "eventSource": "aws:s3",
        "awsRegion": "us-east-1",
        "eventTime": "2021-08-31T23:21:18.663Z",
        "eventName": "ObjectCreated:Put",
        "userIdentity": {
          "principalId": "AWS:AIDA3F7J3O6VSSMGF3L5I"
        },
        "requestParameters": {
          "sourceIPAddress": "37.19.196.22"
        },
        "responseElements": {
          "x-amz-request-id": "F7DE276TBHF29VZD",
          "x-amz-id-2": "qUFk3fP1M5xQawlhrbHSDXN+3nTlpVaKKh4ofmmfA8UeKrg7SND2/IzMqmVToWvDCAp3SgvFboql+ZLp3jCsveutHAOgZk6/"
        },
        "s3": {
          "s3SchemaVersion": "1.0",
          "configurationId": "MjUyNjU5NmItYzdhZi00Y2VkLWE4ODUtYzUzMmQxYzUzN2Fh",
          "bucket": {
            "name": "newspapercdkstack-newspaperdatapipeline098b5a34-17iv1rdoifwdc",
            "ownerIdentity": {
              "principalId": "AGAYO2ZB4RXBE"
            },
            "arn": "arn:aws:s3:::newspapercdkstack-newspaperdatapipeline098b5a34-17iv1rdoifwdc"
          },
          "object": {
            "key": "converted/1921-06-01/1920120901-in_fairbanks_ver01-sn87055779-00296021544-1.jpg",
            "size": 13160923,
            "eTag": "0d5ca70dcfc56056309f0440c50140fe",
            "sequencer": "00612EB97434135321"
          }
        }
      }
    ]
  }

# When we run on AWS, we pass the queue url as an environment variable
sqs_url = os.environ.get('SQS_URL')

if __name__ == "__main__":
  # To run locally use: 
  # docker build -t detect . && docker run -itv ~/.aws/:/home/appuser/.aws:ro -e AWS_PROFILE=default detect

  # When new files are put into s3, a lambda will be triggered to
  #  add a message to the SQS queue. When this program is run, it
  #  checks the queue to find the work that is waiting for it.
  #  We could just run this guy all the time, but the ec2 instance we
  #  are going to be using it kind of expensive to just let idle.
  #  When we finish all the work in this queue, we will shutdown 
  #  this instance.
  sqs = boto3.client('sqs', region_name='us-east-1')
  max_messages = 1
  s3 = boto3.client('s3', region_name='us-east-1')

  run_local = False
  if not run_local:
    # if running on aws
    os.chdir('/tmp')
  else:
    sqs_url = create_sqs_queue_for_local('test_process_pages_queue')
    terminate_sqs_url = create_sqs_queue_for_local('test_terminate_process_pages_queue')
    test_message = build_test_message()
    response = sqs.send_message(QueueUrl=sqs_url,MessageBody=json.dumps(test_message))

  logging.basicConfig(filename="process_pages.txt", level=logging.INFO)
  logging.info("--------------------------------------------")
  logging.info("(%s) process_pages started" % datetime.datetime.today())
  print("(%s) process_pages started" % datetime.datetime.today())
  print("sqs_url: {}".format(sqs_url))

  response = sqs.receive_message(QueueUrl=sqs_url, MaxNumberOfMessages=max_messages)
  messages = response.get('Messages')
  print('{}'.format(messages))

  if messages:
    print("Building model...", end='')
    predictor = build_model()
    print("OK!")

    while messages:
      for message in messages:
        try:
          print("{}\n\n".format(message))
          body = json.loads(message['Body'])
          print("{}\n\n".format(body))
          inner_message = json.loads(body['Message'])
          print("{}\n\n".format(inner_message))
          if inner_message.get('Event') == 's3:TestEvent':
            # Just get rid of "TestEvent" Messages
            # Delete received message from queue
            sqs.delete_message(
                QueueUrl=sqs_url,
                ReceiptHandle=receipt_handle
            )
          else:
            records = inner_message['Records']
            print("{}\n\n".format(records))
            record = records[0]
            print("{}\n\n".format(record))
            bucket_name = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]
            print("{} - {}\n\n".format(bucket_name, key))

            local_name = key.split('/')[-1]
            s3.download_file(bucket_name, key, local_name)

            json_name = local_name.replace('.jpg', '-predictions.json')
            json_key = key.replace('.jpg', '-predictions.json')

            receipt_handle = message['ReceiptHandle']

            response = {'source':key}
            try:
              (response['result'], response['json']) = process_page(local_name)
            except Exception as e:
              print("(%s) Error: %s" % (datetime.datetime.today(), e))
              response['result'] = 'error'
              response['error'] = e
            finally:
              print("(%s) Result: %s" % (datetime.datetime.today(), response['result']))
              #Push notification somewhere?
              # r.rpush(result_queue, json.dumps(response))

            if response['result'] == 'success':
              print("{} successfully processed".format(local_name))
              s3.upload_file(json_name, bucket_name, json_key)
              # Delete received message from queue
              sqs.delete_message(
                  QueueUrl=sqs_url,
                  ReceiptHandle=receipt_handle
              )
        except Exception as e:
            print("(%s) Error: %s" % (datetime.datetime.today(), e))
            response['result'] = 'error'
            response['error'] = e

      # Get the next message in the queue to restart loop
      response = sqs.receive_message(QueueUrl=sqs_url, MaxNumberOfMessages=max_messages)
      messages = response.get('Messages')


