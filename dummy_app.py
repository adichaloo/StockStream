from flask import Flask, render_template, make_response, Response
from datetime import datetime
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
import pyspark
from pyspark.sql import SparkSession
import matplotlib
import matplotlib.pyplot as plt
from io import BytesIO
matplotlib.use('agg')


data = [{'1. open': '143.0500', '2. high': '143.3650', '3. low': '141.8020', '4. close': '141.9100', '5. volume': '4226563', 'stock': 'IBM'},
 {'1. open': '141.5000', '2. high': '142.3100', '3. low': '141.2000', '4. close': '141.8700', '5. volume': '3656559', 'stock': 'IBM'},
 {'1. open': '141.7000', '2. high': '142.0900', '3. low': '140.5600', '4. close': '140.6400', '5. volume': '3285347', 'stock': 'IBM'},
 {'1. open': '141.0100', '2. high': '142.6600', '3. low': '140.6000', '4. close': '140.6600', '5. volume': '3742058', 'stock': 'IBM'},
 {'1. open': '140.0000', '2. high': '141.8300', '3. low': '139.7600', '4. close': '141.4100', '5. volume': '3915480', 'stock': 'IBM'},
 {'1. open': '141.4200', '2. high': '142.3900', '3. low': '141.1100', '4. close': '142.2800', '5. volume': '2937781', 'stock': 'IBM'},
 {'1. open': '142.6600', '2. high': '143.2250', '3. low': '141.3000', '4. close': '141.4900', '5. volume': '3557734', 'stock': 'IBM'},
 {'1. open': '141.7200', '2. high': '143.4750', '3. low': '141.5800', '4. close': '143.4100', '5. volume': '2559083', 'stock': 'IBM'},
 {'1. open': '143.5050', '2. high': '144.4700', '3. low': '143.2200', '4. close': '143.5500', '5. volume': '2900244', 'stock': 'IBM'},
 {'1. open': '144.1800', '2. high': '145.4700', '3. low': '143.5000', '4. close': '145.3500', '5. volume': '3660147', 'stock': 'IBM'},
 {'1. open': '145.4100', '2. high': '146.7400', '3. low': '145.2100', '4. close': '146.0200', '5. volume': '3561347', 'stock': 'IBM'},
 {'1. open': '146.3000', '2. high': '146.7300', '3. low': '145.6200', '4. close': '146.4500', '5. volume': '2778113', 'stock': 'IBM'},
 {'1. open': '146.4200', '2. high': '146.9200', '3. low': '145.7452', '4. close': '146.8600', '5. volume': '2245402', 'stock': 'IBM'},
 {'1. open': '146.9400', '2. high': '147.7275', '3. low': '146.5400', '4. close': '146.8300', '5. volume': '3885949', 'stock': 'IBM'},
 {'1. open': '147.2600', '2. high': '148.1000', '3. low': '146.9200', '4. close': '147.9400', '5. volume': '2727796', 'stock': 'IBM'},
 {'1. open': '147.9100', '2. high': '149.0000', '3. low': '147.5719', '4. close': '148.1300', '5. volume': '3731281', 'stock': 'IBM'},
 {'1. open': '147.6600', '2. high': '148.3300', '3. low': '147.1200', '4. close': '148.0600', '5. volume': '2932203', 'stock': 'IBM'},
 {'1. open': '16.9600', '2. high': '17.0950', '3. low': '16.9400', '4. close': '16.9900', '5. volume': '4037576', 'stock': 'INFY'},
 {'1. open': '17.0800', '2. high': '17.2900', '3. low': '17.0800', '4. close': '17.2700', '5. volume': '6096956', 'stock': 'INFY'},
 {'1. open': '17.3000', '2. high': '17.3400', '3. low': '17.0900', '4. close': '17.1300', '5. volume': '4708806', 'stock': 'INFY'},
 {'1. open': '17.1800', '2. high': '17.3050', '3. low': '17.0600', '4. close': '17.2500', '5. volume': '4662011', 'stock': 'INFY'},
 {'1. open': '17.2600', '2. high': '17.4000', '3. low': '17.2500', '4. close': '17.3300', '5. volume': '5356614', 'stock': 'INFY'},
 {'1. open': '17.2800', '2. high': '17.4300', '3. low': '17.2600', '4. close': '17.4200', '5. volume': '7928080', 'stock': 'INFY'},
 {'1. open': '17.4700', '2. high': '17.5850', '3. low': '17.4500', '4. close': '17.5500', '5. volume': '4613448', 'stock': 'INFY'},
 {'1. open': '17.5000', '2. high': '17.5700', '3. low': '17.3600', '4. close': '17.3700', '5. volume': '6511594', 'stock': 'INFY'},
 {'1. open': '17.5200', '2. high': '17.5775', '3. low': '17.4500', '4. close': '17.5200', '5. volume': '5315228', 'stock': 'INFY'},
 {'1. open': '17.8200', '2. high': '17.9524', '3. low': '17.7600', '4. close': '17.7700', '5. volume': '5746380', 'stock': 'INFY'},
 {'1. open': '17.6900', '2. high': '17.7500', '3. low': '17.5450', '4. close': '17.5800', '5. volume': '5049269', 'stock': 'INFY'},
 {'1. open': '148.1300', '2. high': '148.7800', '3. low': '147.4000', '4. close': '147.5200', '5. volume': '3333040', 'stock': 'IBM'},
 {'1. open': '147.3500', '2. high': '148.5900', '3. low': '147.2600', '4. close': '147.6800', '5. volume': '3722927', 'stock': 'IBM'},
 {'1. open': '148.5700', '2. high': '148.7800', '3. low': '147.5800', '4. close': '148.3800', '5. volume': '3273720', 'stock': 'IBM'},
 {'1. open': '147.9200', '2. high': '148.0000', '3. low': '145.8000', '4. close': '146.3000', '5. volume': '4457695', 'stock': 'IBM'},
 {'1. open': '145.9500', '2. high': '146.9800', '3. low': '145.9200', '4. close': '146.5500', '5. volume': '2627999', 'stock': 'IBM'},
 {'1. open': '147.3800', '2. high': '147.7300', '3. low': '146.4800', '4. close': '147.3500', '5. volume': '2723200', 'stock': 'IBM'},
 {'1. open': '147.1100', '2. high': '147.8500', '3. low': '145.5300', '4. close': '145.9900', '5. volume': '6234033', 'stock': 'IBM'},
 {'1. open': '145.7700', '2. high': '146.4800', '3. low': '145.0600', '4. close': '145.0900', '5. volume': '2508062', 'stock': 'IBM'},
 {'1. open': '145.0000', '2. high': '146.7200', '3. low': '144.6600', '4. close': '146.5200', '5. volume': '3945423', 'stock': 'IBM'},
 {'1. open': '148.3600', '2. high': '151.9299', '3. low': '148.1300', '4. close': '149.8300', '5. volume': '9636681', 'stock': 'IBM'},
 {'1. open': '149.0000', '2. high': '149.2500', '3. low': '147.3100', '4. close': '147.3800', '5. volume': '4944786', 'stock': 'IBM'},
 {'1. open': '147.4100', '2. high': '148.1000', '3. low': '146.8200', '4. close': '146.9100', '5. volume': '2562216', 'stock': 'IBM'},
 {'1. open': '146.5700', '2. high': '147.4300', '3. low': '146.2500', '4. close': '146.4800', '5. volume': '2694245', 'stock': 'IBM'},
 {'1. open': '145.5100', '2. high': '146.1700', '3. low': '143.0201', '4. close': '143.2400', '5. volume': '4824654', 'stock': 'IBM'},
 {'1. open': '143.6700', '2. high': '143.8200', '3. low': '141.7600', '4. close': '143.1700', '5. volume': '4439121', 'stock': 'IBM'},
 {'1. open': '142.1400', '2. high': '142.2820', '3. low': '140.2050', '4. close': '141.5800', '5. volume': '5783422', 'stock': 'IBM'},
 {'1. open': '142.0000', '2. high': '142.1300', '3. low': '139.6100', '4. close': '140.3000', '5. volume': '5703983', 'stock': 'IBM'},
 {'1. open': '140.0400', '2. high': '141.4500', '3. low': '139.8600', '4. close': '140.8000', '5. volume': '3275461', 'stock': 'IBM'},
 {'1. open': '140.8700', '2. high': '141.6400', '3. low': '140.0000', '4. close': '140.3900', '5. volume': '3284421', 'stock': 'IBM'},
 {'1. open': '140.3700', '2. high': '141.2004', '3. low': '139.9900', '4. close': '141.0700', '5. volume': '2637779', 'stock': 'IBM'},
 {'1. open': '140.9000', '2. high': '141.7000', '3. low': '140.1900', '4. close': '141.5200', '5. volume': '3223910', 'stock': 'IBM'},
 {'1. open': '141.4000', '2. high': '142.9400', '3. low': '140.1100', '4. close': '142.0300', '5. volume': '3511347', 'stock': 'IBM'},
 {'1. open': '142.3000', '2. high': '142.4000', '3. low': '140.6800', '4. close': '142.2000', '5. volume': '2354396', 'stock': 'IBM'},
 {'1. open': '142.6000', '2. high': '143.4150', '3. low': '141.7200', '4. close': '142.1100', '5. volume': '3015784', 'stock': 'IBM'},
 {'1. open': '142.5100', '2. high': '143.3400', '3. low': '142.1400', '4. close': '143.2300', '5. volume': '2511459', 'stock': 'IBM'},
 {'1. open': '142.5100', '2. high': '142.9300', '3. low': '140.9500', '4. close': '141.2400', '5. volume': '3921142', 'stock': 'IBM'},
 {'1. open': '139.7700', '2. high': '140.1200', '3. low': '138.2700', '4. close': '138.4600', '5. volume': '4583553', 'stock': 'IBM'},
 {'1. open': '139.2800', '2. high': '139.7800', '3. low': '138.5200', '4. close': '139.2100', '5. volume': '3361468', 'stock': 'IBM'},
 {'1. open': '137.1200', '2. high': '140.6200', '3. low': '136.3100', '4. close': '140.3200', '5. volume': '4172822', 'stock': 'IBM'},
 {'1. open': '140.0000', '2. high': '140.4300', '3. low': '139.5800', '4. close': '139.9700', '5. volume': '3329985', 'stock': 'IBM'},
 {'1. open': '138.6400', '2. high': '139.4050', '3. low': '137.9300', '4. close': '138.0100', '5. volume': '5314159', 'stock': 'IBM'},
 {'1. open': '138.1500', '2. high': '139.2700', '3. low': '137.1200', '4. close': '137.1600', '5. volume': '4865615', 'stock': 'IBM'},
 {'1. open': '136.6300', '2. high': '137.6800', '3. low': '135.8700', '4. close': '136.3800', '5. volume': '3457527', 'stock': 'IBM'},
 {'1. open': '136.7400', '2. high': '137.9800', '3. low': '136.0500', '4. close': '137.7900', '5. volume': '3697975', 'stock': 'IBM'},
 {'1. open': '137.5000', '2. high': '138.4900', '3. low': '136.3300', '4. close': '137.0800', '5. volume': '6472549', 'stock': 'IBM'},
 {'1. open': '142.2000', '2. high': '144.4100', '3. low': '141.5800', '4. close': '143.7600', '5. volume': '11130170', 'stock': 'IBM'},
 {'1. open': '143.6200', '2. high': '144.7000', '3. low': '141.7100', '4. close': '142.5200', '5. volume': '5469227', 'stock': 'IBM'},
 {'1. open': '143.1900', '2. high': '144.5000', '3. low': '142.5800', '4. close': '142.6300', '5. volume': '4204190', 'stock': 'IBM'},
 {'1. open': '143.0000', '2. high': '144.7600', '3. low': '142.5900', '4. close': '144.6400', '5. volume': '6592041', 'stock': 'IBM'},
 {'1. open': '145.0000', '2. high': '146.5100', '3. low': '144.4500', '4. close': '145.4000', '5. volume': '4750081', 'stock': 'IBM'},
 {'1. open': '145.7700', '2. high': '147.1000', '3. low': '144.8400', '4. close': '147.0100', '5. volume': '3902657', 'stock': 'IBM'},
 {'1. open': '147.4500', '2. high': '148.4450', '3. low': '147.2800', '4. close': '147.9000', '5. volume': '3510495', 'stock': 'IBM'},
 {'1. open': '147.8900', '2. high': '149.2250', '3. low': '147.8500', '4. close': '148.9700', '5. volume': '4597249', 'stock': 'IBM'},
 {'1. open': '149.0300', '2. high': '149.2800', '3. low': '148.0300', '4. close': '148.8300', '5. volume': '3549853', 'stock': 'IBM'},
 {'1. open': '149.2500', '2. high': '149.6800', '3. low': '147.5850', '4. close': '148.0300', '5. volume': '3618588', 'stock': 'IBM'},
 {'1. open': '146.5500', '2. high': '146.9900', '3. low': '145.2800', '4. close': '146.6200', '5. volume': '3412713', 'stock': 'IBM'},
 {'1. open': '147.4400', '2. high': '149.1700', '3. low': '146.8500', '4. close': '149.0200', '5. volume': '3179541', 'stock': 'IBM'},
 {'1. open': '148.4600', '2. high': '148.4900', '3. low': '147.3500', '4. close': '148.1000', '5. volume': '2647288', 'stock': 'IBM'},
 {'1. open': '149.4500', '2. high': '150.8063', '3. low': '149.0500', '4. close': '150.4100', '5. volume': '4321940', 'stock': 'IBM'},
 {'1. open': '150.4000', '2. high': '153.2200', '3. low': '150.4000', '4. close': '152.5800', '5. volume': '4632519', 'stock': 'IBM'},
 {'1. open': '153.0000', '2. high': '153.3500', '3. low': '152.1300', '4. close': '153.0600', '5. volume': '3519172', 'stock': 'IBM'},
 {'1. open': '153.2900', '2. high': '153.5000', '3. low': '152.4601', '4. close': '152.8900', '5. volume': '4426676', 'stock': 'IBM'},
 {'1. open': '152.5100', '2. high': '154.6800', '3. low': '152.3500', '4. close': '154.3500', '5. volume': '3658936', 'stock': 'IBM'},
 {'1. open': '154.6000', '2. high': '154.6600', '3. low': '153.5100', '4. close': '153.9100', '5. volume': '2859508', 'stock': 'IBM'},
 {'1. open': '154.5000', '2. high': '155.7050', '3. low': '154.1600', '4. close': '155.1300', '5. volume': '3045091', 'stock': 'IBM'},
 {'1. open': '155.1300', '2. high': '155.4000', '3. low': '153.9200', '4. close': '155.1800', '5. volume': '1799161', 'stock': 'IBM'},
 {'1. open': '154.9900', '2. high': '156.1350', '3. low': '154.7500', '4. close': '155.5700', '5. volume': '4053093', 'stock': 'IBM'},
 {'1. open': '155.4400', '2. high': '155.7450', '3. low': '154.8600', '4. close': '155.6500', '5. volume': '2666182', 'stock': 'IBM'},
 {'1. open': '156.1500', '2. high': '157.5100', '3. low': '156.0200', '4. close': '156.4100', '5. volume': '3568887', 'stock': 'IBM'},
 {'1. open': '156.9500', '2. high': '158.6000', '3. low': '156.8900', '4. close': '158.5600', '5. volume': '6740622', 'stock': 'IBM'},
 {'1. open': '158.4100', '2. high': '160.5900', '3. low': '158.0000', '4. close': '160.5500', '5. volume': '4871860', 'stock': 'IBM'}]


stock_data = {},
plot_data1_IBM = []



app = Flask(__name__)
socketio = SocketIO(app)
Consumer = KafkaConsumer('stock_data')


def kafka_consumer():
    for message in Consumer:
        print("Hello test")
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
            print("HELLLo")
            plot_data1_IBM.append(float(dlist[3]))
        # print(f"consumer triggered: {row_values}")
        if row_values["stock"] in stock_data:
            stock_data[row_values["stock"]].append(row_values)
        else:
            stock_data[row_values["stock"]] = [row_values]

        socketio.emit("update_data", row_values)


@app.route('/')
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

    

    plt.gcf().autofmt_xdate()

    # Create a Matplotlib plot
    fig, ax = plt.subplots()
    ax.plot(plot_data1_IBM)
    ax.set_title('Dynamic Matplotlib Plot')

    # Save the plot to a BytesIO buffer
    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    plt.close()

    # Return the buffer as an image response
    return Response(buffer.getvalue(), mimetype='image/png')

if __name__ == '__main__':
        
        with ThreadPoolExecutor(max_workers=2) as executor:
        # Submit tasks for each stock
            futures = [executor.submit(kafka_consumer)]

        # Wait for all tasks to complete
        # for future in futures:
        #     future.result()
            socketio.run(app, debug=True, port=8087)

        # app.run(debug = True, port = 8084)


