 # StockStream #
Live StockMarket Analysis 

## Details for Execution of Project ##
  * Installation of Kafka [Refer this link if process is not clear: https://medium.com/@the_infinity/apache-kafka-for-beginners-learn-kafka-by-hands-on-cc0bba8059cb]
    * mkdir kafka
    * cd kafka
    * wget https://downloads.apache.org/kafka/3.6.0/kafka_2.12-3.6.0.tgz
    * tar -xvzf kafka_2.12-3.6.0.tgz
    * cd kafka_2.12-3.6.0

      
  * Run Kafka and Zookeeper in the kafka_2.12-3.6.0 folder [Run the 2 commands on separate terminals]
    * Run Zookeeper : bin/zookeeper-server-start.sh config/zookeeper.properties
    * Run kafka server: bin/kafka-server-start.sh config/server.properties
  
  * Now start the applications by running the commands [Make sure the Zookeeper and kafka are running]
    * cd StockStream 
    * Install dependencies: pip install -r requirements.txt
    * On one terminal run the flask application: python app.py
    * On another terminal run the producer : python producer.py
  
  * Hit http://localhost:8080/data on the browser to see the analysis

