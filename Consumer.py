
"""
@Author: Rikesh Chhetri
@Date: 2021-09-12 
@Last Modified by: Rikesh Chhetri
@Last Modified time: 2021-09-12 10:03:30
@Title : Program Aim perform the Prediction of Live Stock Data and Receiving Of Message From
         Kafka Producer And Processing and THose Data Using Spark And MlLib
"""

import pandas as pd
import json
from kafka import KafkaConsumer
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions
from logging_handler import logger
from pyspark.sql import SparkSession

spark= SparkSession.builder.appName('Stock Data processing').getOrCreate()

try:
    Consumer = KafkaConsumer('stock_data')
except Exception as e:
    logger.error(e)

def StockPricePrediction():
    try:
        for message in Consumer:
            
            res = json.loads(message.value.decode('utf-8'))
            dlist = list(res.values())
            print(f"consumer triggered: {dlist}")
            df = pd.DataFrame([dlist], columns=['Open', 'High','Low','Close','Volume'])
            df = df.astype(float)
            spark_df = spark.createDataFrame(df)
            vectorAssembler = VectorAssembler(inputCols=['Open', 'High', 'Low'], outputCol='features')
            df_vect = vectorAssembler.transform(spark_df)
            df_vect_features = df_vect.select(['features', 'Close'])
            # predictions = LoadModel.transform(df_vect_features)
            # predictions.select("prediction", "Close", "features").show()
            # predict_value = predictions.select(functions.round(predictions["prediction"], 2).alias("prediction")).collect()[0].__getitem__("prediction")
            # close_value = predictions.select('Close').collect()[0].__getitem__('Close')
            # print(message.key)
            # date_time = message.key.decode('utf-8')
    
    except Exception as e:
        logger.error(e)



StockPricePrediction()
