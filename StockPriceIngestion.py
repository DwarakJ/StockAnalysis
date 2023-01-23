import json
import boto3
import yfinance as yf
import datetime


class StockIngestion:
    def __init__(self, kinesis_client, start_datetime, end_datetime, stockstream, chunk_size, stocks):
        self.kinesis = kinesis_client
        self.stream_name = stockstream
        self.start = start_datetime
        self.end = end_datetime
        self.chunk_size = chunk_size

        ## Using chunk counter to increment in the below code and send data to kinesis once chunk_counter == chunk_size
        self.chunk_counter = 0

        ## Add code to pull the data for the stocks specified in the doc
        self.stocks = stocks

        self.chunk_data = []

    def get_stock_data(self):
        ## Iterating over each stock name
        for stock in self.stocks:

            ## Pulling stock data using yahoo finance api
            data = yf.download(stock, start= self.start, end= self.end, interval = '1h' )
            print(f"Retrieving stock data for {stock}")

            ## Pulling info data for each stock to get 52_week_high, 52_week_low data
            info_data = yf.Ticker(stock).info

            ## Adding Stock name, 52_week_high, 52_week_low info to 'data' dataframe
            data['stock'] = stock
            data['52_week_high'] = info_data['fiftyTwoWeekHigh']
            data['52_week_low'] = info_data['fiftyTwoWeekLow']

            ## Uncomment below statement to print stock data from yahoo finance api
            #print(data)

            ## Renaming Close column name to price
            data.rename(columns = {'Close':'price'}, inplace = True)

            ## Converting data frame to json to get the right format to send to kinesis
            stock_data = json.loads(data[['stock', 'price', '52_week_high', '52_week_low']].to_json(orient='index', date_unit="s"))

            for timestamp in stock_data:
                self.chunk_counter += 1

                ## Building stream_data for each stock and timestamp
                stream_data = {
                'stock': stock,
                'timestamp': timestamp,
                'price': stock_data[timestamp]["price"],
                '52_week_high': stock_data[timestamp]["52_week_high"],
                "52_week_low": stock_data[timestamp]["52_week_low"],
                }

                print(stream_data)

                ## Adding stream data to a list to create a batch of records to send to kinesis
                self.chunk_data.append(stream_data)
                
                if self.chunk_counter == self.chunk_size:

                    ## Pushing data to kinesis
                    self.put_data_to_kinesis(self.stream_name, self.chunk_data, stock)
                    
                    self.chunk_data = []
                    self.chunk_counter = 0

            ## Pushing remaning data in chunk_data list to kinesis
            if self.chunk_counter != 0:
                self.put_data_to_kinesis(self.stream_name, self.chunk_data, stock)
                self.chunk_data = []
                self.chunk_counter = 0

        
    def put_data_to_kinesis(self, stream_name, chunk_data, stock):
        return self.kinesis.put_record(
                        StreamName=stream_name,
                        Data=json.dumps(chunk_data),
                        PartitionKey=stock)
        
if __name__=='__main__':
    kinesis_client = boto3.client('kinesis', region_name = "us-east-1") 
    stocks = ["MSFT", "MVIS", "GOOG", "SPOT", "INO", "OCGN", "ABML", "RLLCF", "JNJ", "PSFE"]
    start_datetime = datetime.date.today() - datetime.timedelta(4)
    end_datetime = datetime.date.today() - datetime.timedelta(3)
    kinesis_data_stream_name = "stock_stream"

    ## Setting the chunk size here to limit the list of stream data to sent to kinesis
    chunk_size = 100

    stock_ingestion = StockIngestion(kinesis_client, start_datetime, end_datetime, kinesis_data_stream_name, chunk_size, stocks)
    stock_ingestion.get_stock_data()
