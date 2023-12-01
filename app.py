"""
@Author: Rikesh Chhetri
@Date: 2021-09-12 
@Last Modified by: Rikesh Chhetri
@Last Modified time: 2021-09-12 10:03:30
@Title : Program Aim perform the Visualization Of Predicted Values Live On Webpage Using Flask and HighChart js
"""

from flask import Flask, render_template, make_response
from datetime import datetime
import time
import json
import Consumer
import sys
from logging_handler import logger

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/data')
def data():
    try:
        data = Consumer.StockPricePrediction()
        print("HELOooooooo")
        response = make_response(json.dumps([data]))
        response.content_type = 'application/json'
        time.sleep(2)
        return response
    except Exception as e:
        print(f"HELOooooooo error: {e}")
        logger.info(e)
        sys.exit(1)


if __name__ == ("__main__"):
    app.run(debug=True, port=8080)
