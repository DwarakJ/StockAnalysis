from pprint import pprint
import boto3
import json
import datetime
import base64
from decimal import Decimal


def lambda_handler(event, context):
    dynamodb_res = boto3.resource('dynamodb', region_name='us-east-1')

    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data'])
        partitionKey = base64.b64decode(record['kinesis']['partitionKey'])
        payload = str(payload, 'utf-8')
        # pprint(payload, sort_dicts=False)

        # print(payload)

        payload = json.loads(payload, parse_float=Decimal)
        #pprint(payload, sort_dicts=False)

        alert_counter = 0

        for hour in payload:

            if (payload[hour]["price"] >= ((payload[hour]["52_week_high"] * 80)/100)) or (payload[hour]["price"] <= ((payload[hour]["52_week_low"] * 120)/100)):
                print(datetime.datetime.fromtimestamp(int(hour)).strftime('%Y-%m-%d %H:%M:%S'), payload[hour])

                table = dynamodb_res.Table('Stock_POI_Data')
                print(table.put_item(Item=payload[hour]))

                if alert_counter < 1:
                    client = boto3.client('sns', region_name='us-east-1')
                    topic_arn = "arn:aws:sns:us-east-1:661615427631:stock_poi_sns"
                    alert_counter += 1

                    try:
                        client.publish(TopicArn=topic_arn, Message=str(payload[hour]["price"]), Subject="Stock Point of Interest")
                        print("Notification sent successfully")
                    except Exception as ex:
                        print(f"Issue in sending notification {ex}")
                