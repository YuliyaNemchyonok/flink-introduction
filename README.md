# Flink Introduction Project

This project demonstrates several functionalities that can be achieved using Apache Flink. It includes examples of how
to read data from a text file, filter data, and write the results to a sink. It also shows how to consume data from a
Kafka topic, apply transformations, and write the results to a sink.

The main goal of this project is to encourage you to experiment with different parameters of Apache Flink and understand
their impact on the performance and behavior of your Flink applications.

## Prerequisites

To run this project, you need to have the following:

- A running Apache Flink cluster
- A running Kafka cluster

## Installation

Follow the below tutorials to set up Apache Flink and Kafka:

- [Apache Flink Installation](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/deployment/resource-providers/standalone/overview/#starting-a-standalone-cluster-session-mode)
- [Apache Kafka Installation](https://kafka.apache.org/quickstart)

## Running the Project

1. Clone the repository: `git clone https://github.com/YuliyaNemchyonok/flink-introduction.git`
2. Navigate to the project directory: `cd flink-introduction`
3. Run `mvn clean compile assembly:single`. It will create `flink-intro-test-app.jar` in the `target` folder. Do not
   forget to change `CHANGE_HERE` to paths on your local environment.
4. Place the `test.csv` file in the `source-local-path` destination.
5. Add `flink-intro-test-app.jar` to your Flink cluster
6. Create topic in your Kafka cluster.
   Example `~/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic source3`
7. Start generating data in your Kafka topic.
   Example `~/kafka/bin/kafka-producer-perf-test.sh --producer-props bootstrap.servers=localhost:9092 --topic source3 --num-records 1000000 --record-size 10 --throughput 10`

## Project Structure

The project is structured as follows:

- `src/main/scala/com/eneco/teaching/flink_intro/App.scala`: This is the main application file where the Flink job is
  defined and executed. I advise not to use Scala API for Flink anymore as it is now deprecated.
- `src/main/resources/application.conf`: This file contains the configuration parameters for the application.
