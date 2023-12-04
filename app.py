from flask import Flask, render_template, make_response, Response
from datetime import datetime
import time
import json
import time
import sys
from logging_handler import logger
from flask_socketio import SocketIO
import threading
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType
from pyspark.sql.functions import mean, col, to_date
from pyspark.sql.functions import log
import matplotlib.pyplot as plt
import matplotlib
from io import BytesIO
import pyspark
from pyspark.sql import SparkSession
matplotlib.use('agg')




start_time_new = time.time()
app = Flask(__name__)
socketio = SocketIO(app)
Consumer = KafkaConsumer('stock_data') # Subscribed to the topic named stock_data

spark = (SparkSession.builder
         .appName('Stock Data processing')
         .config("spark.executor.instances", 1)
         # the number of CPU cores memory this needs from the executor,
         # it would be reserved on the worker
         .config("spark.executor.cores", "1")
         .config("spark.executor.memory", "1G")
         .getOrCreate())

stock_data = {}
time_log = []
latency_list=[]


schema = StructType([
    # StructField("timestamp", StringType()),
    StructField("open", FloatType()),
    StructField("high", FloatType()),
    StructField("low", FloatType()),
    StructField("close", FloatType()),
    StructField("volume", FloatType()),
    StructField("stock", StringType())
])

plot_data_IBM = []
plot_data_GE = []
plot_data_INFY = []

# Aggregation of all the stock data contiuously 
def kafka_consumer():
    for message in Consumer:
        received_timestamp = time.time()
        res = json.loads(message.value.decode('utf-8')) # Loading the stock data from the topic
        data = res["data"]
        producer_timestamp = res["timestamp"]
        latency = (received_timestamp - producer_timestamp)*1000
        latency_list.append(latency)
        dlist = list(data.values())
        row_values = {
            # 'timestamp': timestamp,
            'open': float(dlist[0]),
            'high': float(dlist[1]),
            'low': float(dlist[2]),
            'close': float(dlist[3]),
            'volume': float(dlist[4]),
            'stock': str(dlist[5])

        }

        if str(dlist[5]) == "IBM":    
            plot_data_IBM.append(float(dlist[3]))
        elif str(dlist[5]) == "GE":
            plot_data_GE.append(float(dlist[3]))
        elif str(dlist[5]) == "INFY":
            plot_data_INFY.append(float(dlist[3]))

        # Segregation of stock data
        if row_values["stock"] in stock_data:
            stock_data[row_values["stock"]].append(row_values)
        else:
            stock_data[row_values["stock"]] = [row_values]

        socketio.emit("update_data", row_values)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/plot')
def plot():
    plt.gcf().autofmt_xdate()

    # Create a Matplotlib plot
    fig, ax = plt.subplots()
    ax.plot(plot_data_IBM, label='IBM', color='red')
    ax.plot(plot_data_GE, label='GE', color='green')
    ax.plot(plot_data_INFY, label='INFY', color='blue')



    ax.set_title('Dynamic Matplotlib Plot')

    # Save the plot to a BytesIO buffer
    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    plt.close()

    # Return the buffer as an image response
    return Response(buffer.getvalue(), mimetype='image/png')

# Api call for Analysis of processing of 
@app.route('/data')
def data():
    try:
        processing_start_time=time.time()
        df_dict = {}
        for stock in stock_data:
            df_dict[stock] = spark.createDataFrame(stock_data[stock], schema=schema)
        stock_count = 0
        table_data = {}
        for stock in stock_data:
            stock_count += len(stock_data[stock])
            print(f"#### DATA FRAME AND ANALYSIS OF STOCK - {stock} ######")
            df_dict[stock].show()

            # Highest average daily returns
            df_dict[stock] = df_dict[stock].withColumn("daily_return", ((df_dict[stock].close - df_dict[stock].open) / df_dict[stock].open)*100)
            df_dict[stock] = df_dict[stock].orderBy(col("daily_return").desc()).limit(5)

            # What day did the stock reach it’s highest peak price? What was the peak price?
            print(f"{stock} Description of data - What day did the stock reach it’s highest peak price? What was the peak price?")
            print(df_dict[stock].orderBy(df_dict[stock]["high"].desc()).head(1)[0][0])  # print date
            print(df_dict[stock].orderBy(df_dict[stock]["high"].desc()).head(1)[0][2])  # print price

            # Lowest daily return 
            print(f"{stock} lowest daily return")
            df_dict[stock]= df_dict[stock].withColumn("Return", log(df_dict[stock]["close"] / df_dict[stock]["open"]) * 100)
            df_dict[stock].select('Return').describe().show() # Just for log to compare the data with the data rendered on the UI
            summary_df = df_dict[stock].select('Return').describe().toPandas()
            table_data[stock] = {
                "table": summary_df.to_html(classes='table table-bordered table-striped', index=False)
            }

            print("####################")

        processing_end_time = time.time()
        one_record = processing_end_time-processing_start_time
        print(f"Time for the aggregated records - {stock_count} to complete all the tasks is {one_record}")

        # Kafka latency
        average_latency = sum(latency_list) / len(latency_list)
        print(f"Approx Average Latency for Batch of {len(latency_list)} messages: {average_latency} seconds")

        time_log.append({"record_count": stock_count, "processing_time": one_record})

        return render_template('output.html',one_record=one_record,final_record=stock_count, table=table_data, time_table=time_log)
    except Exception as e:

        logger.info(e)
        sys.exit(1)



if __name__ == ("__main__"):
    # Use ThreadPoolExecutor to manage the threads
    with ThreadPoolExecutor(max_workers=2) as executor:
        # Submit tasks for each stock
        futures = [executor.submit(kafka_consumer)]
        socketio.run(app, debug=True, port=8080)

