# Data Streaming Pipeline

## Introduction
This is a data processing pipeline which ingests data (weather data) from an endpoint and drops it in Apache Kafka. Then Apache Spark is used to process the data and stored in Apache Cassandra. The entire pipeline is orchestrated using Apache Airflow with the help of Kafka and Spark providers. Due to the fact that data is available in hourly and daily batches, rather than building a streaming pipeline, this is a batch processing pipeline. This solution can also be used for streaming in which case the batch processing in Spark is tweaked to satisfy the streaming solution but the spark triggers have to be handled in a custom fashion. 
The design of tasks in a DAG is that upstream tasks fully execute successfully before a downstream task executes. That dependency nature does not make it suitable to orchestrate an entire streaming pipeline since in a streaming pipeline, all tasks are running continuously.

## Description
* The Project depends on Airflow for Orchestrating and scheduling. 
* Using python to make the requests to the endpoint to get hourly and daily data metrics. 
* By the help of a kafka provider in Airflow (confluent-kafka), data is produced to the suitable kafka topic using the `ProduceToTopicOperator` 
* The data is then consumed with a spark job by the help of the `SparkSubmitOperator` in the Spark provider in Apache Airflow
* The same Apache Spark job (written in PySpark) connects to write the consumed data into a Cassandra Database


## Running the code (either locally or on a remote system)
* Clone the repository
* You need the various dependencies to run the code. Since the entire architecture is running on docker compose, make sure to install docker and docker compose on your system. 
* Depending on your system resources available to you, check the resource allocations in the `docker-compose.yml` file and the spark jobs in the `dags` directory to adjust accordingly
* Create a `.env` file for the environmental variables to help in the execution. See values below
    * The `BASE_URL` is the URL endpoint before the query parameters
    * The `HOURLY_DATA_TOPIC` is the topic that the hourly data will be published on
    * The `DAILY_DATA_TOPIC` is the topic that the daily metrics data will be published on
    * The `DAILY_SINK_TABLE` is the Apache Cassandra table for the daily metrics data
    * The `HOURLY_SINK_TABLE` is the Apache Cassandra table for the hourly metrics data
    * The `CASSANDRA_KEYSPACE` is the keyspace for the Cassandra database
    * The `AIRFLOW_UID` is the user ID for Airflow. From the Airflow docs, this is from Linux system user so it uses the `50000` as the default for the airflow user.
    * The `EC2_PUBLIC_DNS` is the IP variable for Kafka in case you run the solution on a remote system and want to expose kafka to outside connectivity. 

```bash
BASE_URL=https://{api.open-meteo.com}/v1/dwd-icon
_PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-postgres requests apache-airflow-providers-apache-spark
HOURLY_DATA_TOPIC=hourlymetrics
DAILY_DATA_TOPIC=dailymetrics
DAILY_SINK_TABLE=dailydata
HOURLY_SINK_TABLE=hourlydata
CASSANDRA_KEYSPACE=analytics
AIRFLOW_UID=50000
EC2_PUBLIC_DNS=localhost
```
* After setting the environmental variables, we are ready to run the code. I have included the list of commands in the [startup](startup.sh) file The commands before the `docker-compose up -d` are based on the `Ubuntu` Distribution of the Linux system. In case you are using a different Distribution, you can adjust accordingly. Command
```bash
    sh startup.sh
```
* The above command will trigger the start of the program. 
* In case you wish to see the logs, you can edit the startup script and remove the detach option from the `docker-compose up -d` command to have `docker-compose up`. 
* Wait for the application to fully start and visit the Airflow web App on `http://{HOST-IP_ADDRESS}:8080`.
* There is a [script](script/entrypoint.sh) in `script/entrypoint.sh` that is used to create an airflow user when starting up the Airflow services, there you can find the user and password to login to the Airflow Web App. 
* You can change to a more secure password in that script or after you login to the Airflow web app. 
* On the Web App, you have to create 2 connections, for Kafka and Spark. We are using the default connections so go to `Admin` -> `Connections` and scroll to `kafka_default`. Edit the connection details and in the `config_dict` change the `broker:9092` to `kafka:9092`. Save
* Go to `spark_default` and Edit. The host is `spark://spark-master` and the port is `7077`. Save
* Go back to Dags and you are ready to execute
* The historic DAG is not on schedule so it will require a manual trigger once. 
* You can create Airflow Variables from the UI to pass as the Request parameters. Some values have been passed as the default which you can see in the [utils](dags/utils.py) file. The Variables include `latitudes`, `longitudes`, `hourly`, `daily`, `start_date`, `end_date`. 
* The utils file gives you an idea of how the values should look like. 
* You can now manually trigger the execution of any of the dags. 
* NB: The `start_date` and `end_date` are configured to be used only by the historic run. The main DAG uses the default current day. 

### POST DAG Execution
* After the pipeline successfully executes, you can connect to the cassandra database in the docker environment using the command below. Run the CQLSH commands (similar to SQL which can be found in the Cassandra docs) to explore the data

```bash
sudo docker compose exec -it cassandra_db cqlsh -u cassandra -p cassandra localhost 9042
```


## Tools
* Apache Ariflow
* Cassandra
* Apache Spark
* Apache Kafka
* PostgreSQL
* Confluent Schema Registry

