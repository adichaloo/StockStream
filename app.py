
from flask import Flask, render_template, make_response
from datetime import datetime
import time
import json
import Consumer
import sys
from logging_handler import logger
from flask_socketio import SocketIO
import threading
from kafka import KafkaConsumer


app = Flask(__name__)
socketio = SocketIO(app)
Consumer = KafkaConsumer('stock_data')


@app.route('/')
def index():
    return render_template('index.html')

agg_data = []
def kafka_thread():
    for message in Consumer:
        
        res = json.loads(message.value.decode('utf-8'))
        dlist = list(res.values())
        print(f"consumer triggered: {dlist}")
        agg_data.append(dlist)
        socketio.emit("update_data", dlist)

kafka_thread = threading.Thread(target=kafka_thread)
kafka_thread.start()


@app.route('/data')
def data():
    try:
        # data = Consumer.StockPricePrediction()
        # response = make_response(json.dumps(agg_data))
        # response.content_type = 'application/json'
        # time.sleep(2)
        # return response
        return render_template('output.html',response=agg_data)
    except Exception as e:
        logger.info(e)
        sys.exit(1)


if __name__ == ("__main__"):
    app.run(debug=True, port=8080)
