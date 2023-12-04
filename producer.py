from logging_handler import logger

from time import sleep
from kafka import KafkaProducer
from alpha_vantage.timeseries import TimeSeries
import threading
from concurrent.futures import ThreadPoolExecutor
import json
import sys
import os
import time
from dotenv import load_dotenv

load_dotenv('.env')


def dataGrabber(ticker='TCS'):
    try:
        keys = "Q7Q48LRFF2HR71NT"
        time = TimeSeries(key=keys, output_format='json')
        filename = f"{ticker}_data.json"
        try:
            with open(filename, 'r') as file:
                data = json.load(file)
            print(f"Data loaded from {filename}") # To avoid excessive api calls as there is a certain limit we are loading data on our system if the jsons are not available on the system.
            return data
        except FileNotFoundError:
            pass
        # API call to alphavantage for the stock data
        data, metadata = time.get_daily(symbol=ticker, outputsize='full')
        # Save the data to a JSON file
        with open(filename, 'w') as file:
            json.dump(data, file)
            print(f"Data saved to {filename}")

        return data
    except Exception as e:
        print(f"error: {e}")
        logger.info(e)
        sys.exit(1)


# Publishing the data in json to the topic stock_data
def messagePublisher(producerKey, key, data_key):
    timestamp = time.time()
    keyBytes = bytes(key, encoding='utf-8')
    message = {"data": data_key, "timestamp": timestamp}
    producerKey.send("stock_data", json.dumps(message).encode('utf-8'), keyBytes)
    print(f"Message Published! {data_key}")

#Establish connection with the broker
def kafkaProducerConnect():
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
        return producer
    except Exception as e:
        logger.info(e)


def process_stock(stock):
    data = dataGrabber(stock) # Get the data from Alphavantage
    print(f"Data for {stock}: {len(data)}")
    if len(data) > 0:
        i=0
        kafkaProducer = kafkaProducerConnect() # Establish connection with the broker
        for key in sorted(data):

            temp = data[key]
            temp.update({"stock": stock})
            messagePublisher(kafkaProducer, key, temp) # Push data to a topic
            i+=1
            if i%100==0:
                sleep(3)
            # sleep(1)


if __name__ == "__main__":
    stock_list = ['IBM', 'INFY', 'GE'] # Can add more tickers and perform the tasks
    max_threads = len(stock_list)

    # Use ThreadPoolExecutor to manage the threads
    with ThreadPoolExecutor(max_threads) as executor:
        # Submit tasks for each stock
        futures = [executor.submit(process_stock, stock) for stock in stock_list]
        # Wait for all tasks to complete
        for future in futures:
            future.result()
