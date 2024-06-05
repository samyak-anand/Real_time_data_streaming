"""
Historic Data - Should be executed once without any schedule
"""
import os
import logging
from datetime import datetime, timedelta
from uuid import uuid4

from dotenv import load_dotenv

from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator

from utils import (
    daily_main, hourly_main
)

load_dotenv()

logger = logging.getLogger(__name__)

daily_topic = os.environ.get("DAILY_DATA_TOPIC", "dailymetrics")
hourly_topic = os.environ.get("HOURLY_DATA_TOPIC", "hourlymetrics")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.today(),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    "historic_weather_data_dag",
    default_args=default_args,
    description="Historical Weather metrics data streaming",
    schedule_interval=None,
) as historic_weather_data_dag:

    @task
    def start_task():
        print("Let's start the task")

    # @task

    produce_daily_metrics = ProduceToTopicOperator(
        task_id="produce_daily_metrics",
        kafka_config_id="kafka_default",
        topic=daily_topic,
        producer_function=daily_main,
        poll_timeout=10,
    )

    produce_hourly_metrics = ProduceToTopicOperator(
        task_id="produce_hourly_metrics",
        kafka_config_id="kafka_default",
        topic=hourly_topic,
        producer_function=hourly_main,
        poll_timeout=10,
    )

    spark_processing_daily = SparkSubmitOperator(
        task_id='spark_processor_daily_task',
        conn_id='spark_default',
        application="/opt/airflow/dags/spark_stream_daily.py",
        total_executor_cores=4,
        executor_memory="12g",
        conf={
            "spark.network.timeout": 1000000,
            "spark.executor.heartbeatInterval": 100000,
            "spark.storage.blockManagerSlaveTimeoutMs": 100000,
            "spark.driver.maxResultSize": "20g"
        },
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,com.github.jnr:jnr-posix:3.1.15"
    )

    spark_processing_hourly = SparkSubmitOperator(
        task_id='spark_processor_hourly_task',
        conn_id='spark_default',
        application="/opt/airflow/dags/spark_stream_hourly.py",
        total_executor_cores=4,
        executor_memory="12g",
        conf={
            "spark.network.timeout": 1000000,
            "spark.executor.heartbeatInterval": 100000,
            "spark.storage.blockManagerSlaveTimeoutMs": 100000,
            "spark.driver.maxResultSize": "20g"
        },
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,com.github.jnr:jnr-posix:3.1.15"
    )

    @task
    def done():
        print("Done with the task")

    dummy_start = start_task()
    # main_task = daily_main()
    end_task = done()

    dummy_start >> produce_daily_metrics >> spark_processing_daily >> end_task
    dummy_start >> produce_hourly_metrics >> spark_processing_hourly >> end_task
