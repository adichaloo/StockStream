from flask import Flask, render_template, make_response, Response
from datetime import datetime
import time
import json
import time
# import Consumer
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
Consumer = KafkaConsumer('stock_data')

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
plot_data_TCS = []
plot_data_INFY = []

def kafka_consumer():
    for message in Consumer:
        # print("hello")
        res = json.loads(message.value.decode('utf-8'))
        dlist = list(res.values())
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
            # print("HELLLo")
            plot_data_IBM.append(float(dlist[3]))
        elif str(dlist[5]) == "TCS":
            plot_data_TCS.append(float(dlist[3]))
        elif str(dlist[5]) == "INFY":
            plot_data_INFY.append(float(dlist[3]))


            # print(len(plot_data_IBM))
        # print(f"consumer triggered: {row_values}")
        if row_values["stock"] in stock_data:
            stock_data[row_values["stock"]].append(row_values)
        else:
            stock_data[row_values["stock"]] = [row_values]

        socketio.emit("update_data", row_values)


# @app.route('/')
# def index():
#     print("Index triggered")
#     return render_template('index.html')



@app.route('/data2')
def index():
    return render_template('dummy_index.html')

@app.route('/plot')
def plot():
    # Get new data for the plot
    # plot_list = []
    # for d in data: 
    #     if d["stock"] == "IBM": 
    #         plot_list.append(float(d["4. close"]))
    # print(len(data))

    
    # a = [x for x in range(100)]
    plt.gcf().autofmt_xdate()

    # Create a Matplotlib plot
    fig, ax = plt.subplots()
    # ax.plot(plot_data1_IBM)
    # print(len(plot_data_IBM))
    ax.plot(plot_data_IBM, label='IBM', color='red')
    ax.plot(plot_data_TCS, label='TCS', color='green')
    ax.plot(plot_data_INFY, label='INFY', color='blue')



    ax.set_title('Dynamic Matplotlib Plot')

    # Save the plot to a BytesIO buffer
    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    plt.close()

    # Return the buffer as an image response
    return Response(buffer.getvalue(), mimetype='image/png')





@app.route('/data')
def data():
    try:
        # print("stock_data", stock_data)
        # Perform statistics analysis here
        start_time=time.time()
        # print(start_time)
        df_dict = {}
        for stock in stock_data:
            print(f"Test : {stock_data[stock]}")

            df_dict[stock] = spark.createDataFrame(stock_data[stock], schema=schema)
            print(f"Test done : {df_dict[stock]}")
        print("Test skip")
        stock_count = 0
        table_data = {}
        for stock in stock_data:
            stock_count += len(stock_data[stock])
            print(f"#### DATA FRAMEEE - {stock} ######")
            df_dict[stock].show()
            print("#### DATA FRAMEEE ######")

            # for column in df.columns:
            #     df = df.withColumn(column, col(column).cast("float"))
            #
            # df = df.withColumn("date", to_date(col("timestamp"), 'yyyy-MM-dd HH:mm:ss'))
            df_dict[stock] = df_dict[stock].withColumn("daily_return", ((df_dict[stock].close - df_dict[stock].open) / df_dict[stock].open)*100)
            # df.show()

            # df = df.withColumn("daily_return", (df.close - df.open) / df.open)
            df_dict[stock] = df_dict[stock].orderBy(col("daily_return").desc()).limit(5)
            # What day did the stock reach it’s highest peak price? What was the peak price?
            print(f"{stock} Description of data - What day did the stock reach it’s highest peak price? What was the peak price?")
            print(df_dict[stock].orderBy(df_dict[stock]["high"].desc()).head(1)[0][0])  # print date
            print(df_dict[stock].orderBy(df_dict[stock]["high"].desc()).head(1)[0][2])  # print price
            print("####################")

            print(f"{stock} lowest daily return")
            df_dict[stock]= df_dict[stock].withColumn("Return", log(df_dict[stock]["close"] / df_dict[stock]["open"]) * 100)
    
            df_dict[stock].select('Return').describe().show() # Just for log to compare
            summary_df = df_dict[stock].select('Return').describe().toPandas()
            table_data[stock] = {
                "table": summary_df.to_html(classes='table table-bordered table-striped', index=False)
            }

            print("####################")
            # print(df_dict[stock].show())

        end_time = time.time()
        one_record = end_time-start_time
        final_record = end_time -start_time_new
        # print(f"Time for {len(agg_data)} record {one_record}")
        print(f"Time for all stock records - {stock_count} one hit {one_record}")
        time_log.append({"record_count": stock_count, "processing_time": one_record})

        print("Time for multiple record".format(final_record))
        return render_template('output.html',one_record=one_record,final_record=stock_count, table=table_data, time_table=time_log)
    except Exception as e:

        logger.info(e)
        sys.exit(1)



if __name__ == ("__main__"):
    # Use ThreadPoolExecutor to manage the threads
    with ThreadPoolExecutor(max_workers=2) as executor:
        # Submit tasks for each stock
        futures = [executor.submit(kafka_consumer)]

        # Wait for all tasks to complete
        # for future in futures:
        #     future.result()
        socketio.run(app, debug=True, port=8090)

    # app.run(debug=True, port=8080)

