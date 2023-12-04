from logging_handler import logger

from time import sleep
from kafka import KafkaProducer
from alpha_vantage.timeseries import TimeSeries
import threading
from concurrent.futures import ThreadPoolExecutor
import json
import sys
import os
from dotenv import load_dotenv

load_dotenv('.env')


def dataGrabber(ticker='TCS'):
    try:
        # keys = os.getenv("API_KEY")
        # keys = "TTT3XUSOJC2M3I73"
        keys = "Q7Q48LRFF2HR71NT"
        time = TimeSeries(key=keys, output_format='json')
        # data, metadata = time.get_intraday(symbol=ticker, interval='5min', outputsize='full')
        filename = f"{ticker}_data.json"
        try:
            with open(filename, 'r') as file:
                data = json.load(file)
            print(f"Data loaded from {filename}")
            return data
        except FileNotFoundError:
            pass

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



def messagePublisher(producerKey, key, data_key):
    keyBytes = bytes(key, encoding='utf-8')
    producerKey.send("stock_data", json.dumps(data_key).encode('utf-8'), keyBytes)
    print(f"Message Published! {data_key}")


def kafkaProducerConnect():
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
        return producer
    except Exception as e:
        logger.info(e)


def process_stock(stock):
    data = dataGrabber(stock)
    print(f"Data for {stock}: {len(data)}")
    if len(data) > 0:
        i=0
        kafkaProducer = kafkaProducerConnect()
        for key in sorted(data):

            temp = data[key]
            temp.update({"stock": stock})
            # messagePublisher(kafkaProducer, key, data[key])
            messagePublisher(kafkaProducer, key, temp)
            i+=1
            if i%100==0:
                sleep(2)
            # sleep(1)


if __name__ == "__main__":
    stock_list = ['IBM', 'INFY', 'TCS']
    max_threads = len(stock_list)  # Adjust this number based on your preference

    # Use ThreadPoolExecutor to manage the threads
    with ThreadPoolExecutor(max_threads) as executor:
        # Submit tasks for each stock
        futures = [executor.submit(process_stock, stock) for stock in stock_list]

        # Wait for all tasks to complete
        for future in futures:
            future.result()
