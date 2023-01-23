import boto3
import json
import datetime
import base64
from decimal import Decimal
from boto3.dynamodb.conditions import Attr

def lambda_handler(event, context):
    dynamodb_res = boto3.resource('dynamodb', region_name='us-east-1')
    table_name = "stock_data" # Change the table name as needed

    for record in event['Records']:
        data = base64.b64decode(record['kinesis']['data'])
        data = str(data, 'utf-8')
        table = dynamodb_res.Table(table_name)

        data = json.loads(data, parse_float=Decimal)

        for payload in data:
            print(payload)

            ## Applying the point of interest condition here
            if (payload["price"] >= ((payload["52_week_high"] * 80)/100)) or (payload["price"] <= ((payload["52_week_low"] * 120)/100)):
                print(datetime.datetime.fromtimestamp(int(payload["timestamp"])).strftime('%Y-%m-%d %H:%M:%S'), payload)

                stock_name = payload['stock']

                event_datetime = datetime.datetime.fromtimestamp(int(payload["timestamp"]))
                
                event_date = str(event_datetime.date())

                # checking the previous msg status in table to avoid sending a notification again for the same stock and date.
                previous_msg_status = check_if_alert_already_raised(table, stock_name, event_date)

                # Key column is a composite key with stock name and timestamp, it will be partition key for this table.
                # Since having stock name as partition key will make a duplicate record, this will overwrite the items in the table as we insert the records into the table.
                # To avoid this, we are creating a composite key with stock name and timestamp as Partition_key and timestamp as sort_key. This will make it unique in the table.

                data = {
                    "key": stock_name + "-" + payload["timestamp"],
                    "stock": stock_name,
                    "timestamp": payload["timestamp"],
                    'date': event_date,
                    "price": payload["price"],
                    "52_week_high": payload["52_week_high"],
                    "52_week_low": payload["52_week_low"],
                    "msg_sent": 1, # default value is 1 because we consider the first msg for a stock for that date will be notified to the subscribers.
                }

                print(table.put_item(Item=data))

                if previous_msg_status == 0:
                    # Sending sns notification if no notification was sent already for the same date and stock.
                    client = boto3.client('sns', region_name='us-east-1')
                    topic_arn = "arn:aws:sns:us-east-1:661615427631:stock_poi_sns"

                    try:
                        client.publish(TopicArn=topic_arn, Message=str(payload), Subject="Stock Point of Interest")
                        print("Notification sent successfully")
                    except Exception as ex:
                        print(f"Issue in sending notification {ex}")
                
                
def check_if_alert_already_raised(table, stock_name, event_date):
    
    response = table.scan(FilterExpression=Attr("stock").eq(stock_name) & Attr("date").eq(event_date))
    data = response["Items"]

    while "LastEvaluatedKey" in response:
        response = table.scan(
            ExclusiveStartKey=response["LastEvaluatedKey"],
            FilterExpression=Attr("stock").eq(stock_name),
        )
        data.extend(response["Items"])

    # If record present in the table for same stock name and date, then we have already raised an alert for it. Hence returning 1.
    return 1 if data else 0
    

            