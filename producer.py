
"""
@Author: Rikesh Chhetri
@Date: 2021-09-12 
@Last Modified by: Rikesh Chhetri
@Last Modified time: 2021-09-12 10:03:30
@Title : Program Aim perform the Prediction Of Live Stock Data and using kafka sending of these data to consumer. 
"""

from time import sleep
from kafka import KafkaProducer
from alpha_vantage.timeseries import TimeSeries
import random
import json
import sys
import os
from dotenv import load_dotenv
load_dotenv('.env')
from logging_handler import logger

def dataGrabber():
    try:
        ticker = 'IBM'
        # keys = os.getenv("API_KEY")
        keys = "Q7Q48LRFF2HR71NT"
        time = TimeSeries(key=keys, output_format='json')
        data, metadata = time.get_intraday(symbol=ticker, interval='5min', outputsize='full')
        print("producer data", data)
        return data
    except Exception as e:
        print(f"error: {e}")
        logger.info(e)
        sys.exit(1)


def messagePublisher(producerKey, key, data_key):
    keyBytes = bytes(key, encoding='utf-8')
    producerKey.send("stock_data", json.dumps(data[key]).encode('utf-8'), keyBytes)
    print("Message Published!")


def kafkaProducerConnect():
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
        return producer
    except Exception as e:
        logger.info(e)


if __name__ == "__main__":
    data = dataGrabber()
    print("data", data)
    if len(data) > 0:
        kafkaProducer = kafkaProducerConnect()
        for key in sorted(data):
            messagePublisher(kafkaProducer, key, data[key])
            sleep(3)