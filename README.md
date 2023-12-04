 # StockStream #
Live StockMarket Analysis 

## Details for Execution of Project ##
  * Installation of Kafka [Refer this link if process is not clear: https://medium.com/@the_infinity/apache-kafka-for-beginners-learn-kafka-by-hands-on-cc0bba8059cb]
    * mkdir kafka
    * cd kafka
    * wget https://downloads.apache.org/kafka/3.6.0/kafka_2.12-3.6.0.tgz
    * tar -xvzf kafka_2.12-3.6.0.tgz
    * cd kafka_2.12-3.6.0

      
  * Run Kafka and Zookeeper in the kafka_2.12-3.6.0 folder
    * Run Zookeeper : bin/zookeeper-server-start.sh config/zookeeper.properties
    * Run kafka server: bin/kafka-server-start.sh config/server.properties
  
  * Run this bash script which will install the requirements and get the producer and consumer up and running
    * ./ 

  * Hit http://localhost:8080/data on the browser to see the analysis

