# analysis.py
import logging
import pandas as pd
from cassandra.cluster import Cluster
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import mean_squared_error, mean_absolute_error
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
cassandra_keyspace = "analytics"
daily_data_table_name = "dailydata"


def spark_connect():
    spark = (
        SparkSession.builder.appName("WeatherApp")
        .config("spark.cassandra.connection.host", "cassandra_db")
        .config("spark.cassandra.connection.port", 9042)
        .getOrCreate()
    )
    return spark


def read_from_cassandra():
    spark = spark_connect()
    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=daily_data_table_name, keyspace=cassandra_keyspace) \
        .load()
    df.show()
    return df.toPandas()


def perform_analysis(data):
    y_true = data['temperature_2m']
    y_pred = data['temperature_2m']

    mse = mean_squared_error(y_true, y_pred)
    mae = mean_absolute_error(y_true, y_pred)

    print(f"Mean Squared Error: {mse}")
    print(f"Mean Absolute Error: {mae}")

    return mse, mae


def plot_results(data):
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=data, x='date_time', y='temperature_2m')
    plt.title('Temperature Over Time')
    plt.xlabel('Date Time')
    plt.ylabel('Temperature (2m)')
    plt.show()


if __name__ == "__main__":
    data = read_from_cassandra()
    mse, mae = perform_analysis(data)
    plot_results(data)
