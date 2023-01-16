from pprint import pprint
import boto3
import json
import csv
import datetime
import os
import random
import base64


def lambda_handler(event, context):
    dynamodb_res = boto3.resource('dynamodb', region_name='us-east-1')

    for record in event['records']:
        payload = base64.b64decode(record['data'])
        payload = str(payload, 'utf-8')
        pprint(payload, sort_dicts=False)

        print(payload)

        # payload_rec = json.loads(payload)
        # pprint(payload_rec, sort_dicts=False)

        # table = dynamodb_res.Table('Stock_POI_Data')
        # response = table.put_item(Item=payload_rec)

        # client = boto3.client('sns', region_name='us-east-1')
        # topic_arn = "arn:aws:sns:us-east-1:989319265633:bedside-anomaly"

        # try:
        #     client.publish(TopicArn=topic_arn, Message="Detected point of interest data", Subject="heartrate90+")
        #     result = 1
        # except Exception:
        #     result = 0

    return payload