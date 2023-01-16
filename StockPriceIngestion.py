import json
import boto3
import sys
import yfinance as yf

import time
import random
import datetime
import pandas as pd

# Your goal is to get per-hour stock price data for a time range for the ten stocks specified in the doc. 
# Further, you should call the static info api for the stocks to get their current 52WeekHigh and 52WeekLow values.
# You should craft individual data records with information about the stockid, price, price timestamp, 52WeekHigh and 52WeekLow values and push them individually on the Kinesis stream

kinesis = boto3.client('kinesis', region_name = "us-east-1") #Modify this line of code according to your requirement.

today = datetime.date.today() - datetime.timedelta(3)
yesterday = datetime.date.today() - datetime.timedelta(4)

# Example of pulling the data between 2 dates from yfinance API
#data = yf.download("MSFT", start= yesterday, end= today, interval = '1h' )

#print(data['Close'])
# df = pd.DataFrame(data)

# print(df['Close'])

# data = yf.Ticker("MVIS")
# data.info
## Add code to pull the data for the stocks specified in the doc
stock_prices = {}
stocks = ["MSFT", "MVIS", "GOOG", "SPOT", "INO", "OCGN", "ABML", "RLLCF", "JNJ", "PSFE"]
#stocks = ["MSFT"]

my_stream_name = "stockstream"

for stock in stocks:
    data = yf.download(stock, start= yesterday, end= today, interval = '1h' )
    print(f"Downloading stock data for {stock}")

    info_data = yf.Ticker(stock).info

    data['52_week_high'] = info_data['fiftyTwoWeekHigh']
    data['52_week_low'] = info_data['fiftyTwoWeekLow']

    json_data = json.loads(data[['Close', '52_week_high', '52_week_low']].to_json(orient='index', date_unit="s"))

    data.rename(columns = {'Close':'price'}, inplace = True)

    # for hour in json_data:
    #     if (json_data[hour]["Close"] >= ((json_data[hour]["52_week_high"] * 80)/100)) or (json_data[hour]["Close"] <= ((json_data[hour]["52_week_low"] * 120)/100)):
    #         print(datetime.datetime.fromtimestamp(int(hour)).strftime('%Y-%m-%d %H:%M:%S'), json_data[hour])

    put_response = kinesis.put_record(
                        StreamName=my_stream_name,
                        Data=json.dumps(json_data),
                        PartitionKey=stock)

    print(json_data)

## Add additional code to call 'info' API to get 52WeekHigh and 52WeekLow refering this this link - https://pypi.org/project/yfinance/


## Add your code here to push data records to Kinesis stream.

#putting the json as per the number of chunk we will give in below function 
#Create the list of json and push like a chunk. I am sending 100 rows together
