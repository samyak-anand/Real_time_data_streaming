# Data Streaming Pipeline:

## Overview:

This project aims to demonstrate real-time data streaming using Apache Kafka and Apache Spark. It includes components for generating sample data, producing it to Kafka, and processing it using Spark Streaming. Docker is utilized for containerization, making deployment easier across different environments.
## Real-time streaming data architecture:

## Directory:
Real-time_data_streaming

![Real-time streaming data architecture](https://github.com/samyak-anand/Real_time_data_streaming/assets/107413662/cc867f03-4a85-4d08-ad52-9e4dfe910a33)



│

├── dags/ # Contains Python scripts for data generation(historic_main.py), Kafka producing(main_py), logging configuration(utils.py),Spark streaming(spark_stream_daily.py and spark_stream_hourly.py).

│ ├── historic_main.py# Python script for generating sample data, it include the airflow variables, data range             variables .

│ ├──> main_py # Python script for producing data to Kafka.

│ ├──> utils.py # Configuration file for logging.

│ └──> sspark_stream_daily.py # Python script for processing data using Spark Streaming for daily stream data.

│ └──> spark_stream_hourly.py # Python script for processing data using Spark Streaming for hourly stream data.

├── logs/ # Holds the logs file.

│ └── dag_processor_manager

│ └── scheduler

├── scripts/ # Includes a shell script for handling dependencies.

│ └── entrypoint.sh

│

├── docker-compose.yml # Configuration file for Docker containers.

├── Dockerfile # Dockerfile for building containers.

├── requirements.txt # Lists the project dependencies.

├── .venv # Environment variables file.
└── .gitignore # Specifies intentionally untracked files to be ignored by Git.

├── Real-time streaming data architecture.png # Diagram illustrating real-time streaming data architecture.

└── README.md # Project README providing an overview and instructions.


## Data Source: 
We are fetching data from api. For weather analysis we opted open-metro.com api where we can get historical data. Open-Meteo partners with national weather services to bring you open data with high resolution, ranging from 1 to 11 kilometers. Our powerful APIs intelligently select the most suitable weather models for your specific location, ensuring accurate and reliable forecasts. The Historical Weather API is based on reanalysis datasets and uses a combination of weather station, aircraft, buoy, radar, and satellite observations to create a comprehensive record of past weather conditions. These datasets are able to fill in gaps by using mathematical models to estimate the values of various weather variables. As a result, reanalysis datasets are able to provide detailed historical weather information for locations that may not have had weather stations nearby, such as rural areas or the open ocean.

```bash
# Environmental Variables
base_url = os.environ.get("BASE_URL", "https://api.open-meteo.com/v1/dwd-icon")
daily_topic = os.environ.get("DAILY_DATA_TOPIC", "dailymetrics")
hourly_topic = os.environ.get("HOURLY_DATA_TOPIC", "hourlymetrics")

# Airflow Variables for historic data
latitudes = Variable.get("latitudes", default_var="52.5244,52.3471,53.5507,48.1374,50.1155")
longitudes = Variable.get("longitudes", default_var="13.4105,14.5506,9.993,11.5755,8.6842")
hourly = Variable.get(
    "hourly", default_var="temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation,rain,surface_pressure,temperature_80m")
daily = Variable.get("daily", default_var="weather_code")

# Date Range variables
start_date = Variable.get("start_date", "2024-04-01")
end_date = Variable.get("end_date", "2024-05-28")
```


## Data Structure

Parameter	Data Type 	Description
latitude	Float	Geographical WGS84 coordinates of the location. Multiple coordinates can be comma separated. E.g. &latitude=52.52,48.85&longitude=13.41,2.35. To return data for multiple locations the JSON output changes to a list of structures. CSV and XLSX formats add a column location_id.
longitude	Float	
start_date	DateTime	The time interval to get weather data. A day must be specified as an ISO8601 date (e.g. 2022-12-31).
end_date	DateTime	The time interval to get weather data. A day must be specified as an ISO8601 date (e.g. 2022-12-31).
temperature_2m	Float	Air temperature at 2 meters above ground
relative_humidity_2m	Float	Relative humidity at 2 meters above ground
dew_point_2m	Float	Dew point temperature at 2 meters above ground
apparent_temperature	Float	Apparent temperature is the perceived feels-like temperature combining wind chill factor, relative humidity and solar radiation
surface_pressure	Float	Atmospheric air pressure reduced to mean sea level (msl) or pressure at surface. Typically pressure on mean sea level is used in meteorology. Surface pressure gets lower with increasing elevation.
precipitation	Float	Total precipitation (rain, showers, snow) sum of the preceding hour. Data is stored with a 0.1 mm precision. If precipitation data is summed up to monthly sums, there might be small inconsistencies with the total precipitation amount.
rain	Float	Only liquid precipitation of the preceding hour including local showers and rain from large scale systems.
![image](https://github.com/samyak-anand/Real_time_data_streaming/assets/107413662/05961dc1-7389-450b-a1b9-45df4d52f109)


## Clone Repository: 
To clone this repository, open terminal, type the following command:
```bash
git clone https://github.com/samyak-anand/Real_time_data_streaming.git
```

## Navigate to Project Directory:
Change directory to Real_time_data_streaming, by using the following command.
```bash
cd Real_time_data_streaming
```

## Set Environment Variables:
Ensure that necessary environment variables are configured in the .venv file.

## Verify the Date:
verify the start_date and end_date in [utils.py](dags/utils.py)  file present in dags folder, define under #Data Range Variables
```bash
# Date Range variables
start_date = Variable.get("start_date", "2024-01-01")
end_date = Variable.get("end_date", "2024-05-28")
```
## Visualization:
This is use for analysis of data once the data is completly loaded in Cassandra databse. Here we are analysing data hourly and daily sepreatly. 


## Introduction:
This is a data processing pipeline which ingests data (weather data) from an endpoint and drops it in Apache Kafka. Then Apache Spark is used to process the data and stored in Apache Cassandra. The entire pipeline is orchestrated using Apache Airflow with the help of Kafka and Spark providers. Due to the fact that data is available in hourly and daily batches, rather than building a streaming pipeline, this is a batch processing pipeline. This solution can also be used for streaming in which case the batch processing in Spark is tweaked to satisfy the streaming solution but the spark triggers have to be handled in a custom fashion. 
The design of tasks in a DAG is that upstream tasks fully execute successfully before a downstream task executes. That dependency nature does not make it suitable to orchestrate an entire streaming pipeline since in a streaming pipeline, all tasks are running continuously.

## Description:
* The Project depends on Airflow for Orchestrating and scheduling. 
* Using python to make the requests to the endpoint to get hourly and daily data metrics. 
* By the help of a kafka provider in Airflow (confluent-kafka), data is produced to the suitable kafka topic using the `ProduceToTopicOperator` 
* The data is then consumed with a spark job by the help of the `SparkSubmitOperator` in the Spark provider in Apache Airflow
* The same Apache Spark job (written in PySpark) connects to write the consumed data into a Cassandra Database


## Running the code (either locally or on a remote system):
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
This project aims to demonstrate real-time data streaming using Apache Kafka and Apache Spark. It includes components for generating sample data, producing it to Kafka, and processing it using Spark Streaming. Docker is utilized for containerization, making deployment easier across different environments.
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

### POST DAG Execution:
* After the pipeline successfully executes, you can connect to the cassandra database in the docker environment using the command below. Run the CQLSH commands (similar to SQL which can be found in the Cassandra docs) to explore the data

```bash
sudo docker compose exec -it cassandra_db cqlsh -u cassandra -p cassandra localhost 9042
```


## Tools:
* Apache Ariflow
* Cassandra
* Apache Spark
* Apache Kafka
* PostgreSQL
* Confluent Schema Registry

## Contact:
www.linkedin.com/in/samyak-anand-496a1143
