 # StockStream #
This application analyzes large volume of stock data in near real time with minimal latency using Pyspark and Apache Kafka. The producers push live stock market data (with a fixed frequency) to a topic in Kafka pipeline. We set up a Pyspark consumer which reads this data asynchronously as any event is published to the topic. Pyspark is used to derive useful inisghts in real time on the aggregated historical and current stock data. The insights are rendered live on a browser using Flask. 

Here is an architecture diagram of the application <br> 
![Alt text](https://github.com/mnswdhw/StockStream/blob/main/images/Screenshot%202024-01-09%20at%202.45.09%E2%80%AFAM.png)

## Details for Execution of Project ##
  * Installation of Kafka [Refer this link if process is not clear: https://medium.com/@the_infinity/apache-kafka-for-beginners-learn-kafka-by-hands-on-cc0bba8059cb]
    * mkdir kafka
    * cd kafka
    * wget https://downloads.apache.org/kafka/3.6.0/kafka_2.12-3.6.0.tgz
    * tar -xvzf kafka_2.12-3.6.0.tgz
    * cd kafka_2.12-3.6.0

      
  * Run Kafka and Zookeeper in the kafka_2.12-3.6.0 folder [Run the 2 commands on separate terminals. Make sure you are in kafka/kafka_2.12-3.6.0 directory]
    * Run Zookeeper : bin/zookeeper-server-start.sh config/zookeeper.properties
    * Run kafka server: bin/kafka-server-start.sh config/server.properties
  
  * Now start the applications by running the commands [Make sure the Zookeeper and kafka are running. Run from StockStream directory]
    * cd StockStream 
    * Install dependencies: pip install -r requirements.txt
    * On one terminal run the flask application: python app.py
    * On another terminal run the producer : python producer.py
  
  * Hit http://localhost:8080/data on the browser to see the analysis

## Example Real Time Analytics 
![Alt text](https://github.com/mnswdhw/StockStream/blob/main/images/Screenshot%202024-01-09%20at%203.17.31%E2%80%AFAM.png)
![Alt text](https://github.com/mnswdhw/StockStream/blob/main/images/Screenshot%202024-01-09%20at%203.17.48%E2%80%AFAM.png)

