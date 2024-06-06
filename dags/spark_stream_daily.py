"""
Spark Stream processing daily - Consuming from Kafka
Loading to Cassadra
"""
# Import the necessary packages
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import (
    StringType, StructType,
    DecimalType, TimestampType, ArrayType
)

from dotenv import load_dotenv

from cassandra.cluster import Cluster

load_dotenv()

# Declare needed variables
logger = logging.getLogger(__name__)
daily_topic = os.environ.get("DAILY_DATA_TOPIC", "dailymetrics")
daily_data_table_name = os.environ.get("DAILY_SINK_TABLE", "dailydata")
cassandra_keyspace = os.environ.get("CASSANDRA_KEYSPACE", "analytics")
bootstrap_server = "kafka:9092"

# Table Schema for the daily data table. This can change depending on the type of daily metrics
sample_schema = (
    StructType()
    .add("id", StringType())
    .add("latitude", DecimalType(scale=6))
    .add("longitude", DecimalType(scale=6))
    .add("date_time", TimestampType())
    .add("generationtime_ms", DecimalType(scale=10))
    .add("utc_offset_seconds", DecimalType(scale=2))
    .add("timezone", StringType())
    .add("timezone_abbreviation", StringType())
    .add("elevation", DecimalType(scale=2))
    .add("weather_value", DecimalType())
)

# Convert to an array schema due to the expected data.
# Use the sample_schema if data is a single json but if an array of jsons, use array_schema
array_schema = ArrayType(sample_schema)


def spark_connect():
    """Create the spark connection to be used for other interaction and processing"""
    spark = (
        SparkSession.builder.appName("WeatherApp")
        .config("spark.cassandra.connection.host", "cassandra_db")
        .config("spark.cassandra.connection.port", 9042)
        .getOrCreate()
    )
    # spark.conf.set("spark.sql.shuffle.partitions", 1000)
    return spark


def stop_spark(spark_session):
    """Stop the spark session"""
    spark_session.stop()
    print("Spark exited successfully")


def create_keyspace(session):
    """Create the cassandra keyspace on the database"""
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS analytics
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def read_stream(spark_session):
    """
    Define the subscription to kafka topic and read batch data or stream data
    In reading, conert the json string to a string and explode
    """
    df = spark_session \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_server) \
        .option("subscribe", daily_topic) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()\
        .select(from_json(col("value").cast("string"), array_schema).alias("data"))

    # Run explode on the data into a json column then select that column data
    df_exploded = df.withColumn("json", explode(col("data"))) \
        .select("json.*")

    df_exploded.printSchema()
    df_exploded.show()
    return df_exploded


def write_stream_data():
    """Write the data into the cassandra db table, then stop the spark session """
    spark = spark_connect()
    stream_data = read_stream(spark)
    stream_data.write\
        .option("checkpointLocation", '/tmp/check_point/')\
        .format("org.apache.spark.sql.cassandra")\
        .option("keyspace", cassandra_keyspace)\
        .option("table", daily_data_table_name)\
        .mode("append") \
        .save()

    stop_spark(spark)


def create_cassandra_connection():
    """Cassandra connection that is used to create the keyspace and table"""
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['cassandra_db'], port=9042, )

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logger.error(f"Could not create cassandra connection due to {e}")
        return None


def create_table(session):
    """Create table statement for Cassandra"""
    session.execute(f"""
    CREATE TABLE IF NOT EXISTS {cassandra_keyspace}.{daily_data_table_name} (
        id TEXT PRIMARY KEY,
        latitude DECIMAL,
        longitude DECIMAL,
        date_time DATE,
        generationtime_ms DECIMAL,
        utc_offset_seconds DECIMAL,
        timezone TEXT,
        timezone_abbreviation TEXT,
        elevation DECIMAL,
        weather_value DECIMAL)
    """)

    print("Table created successfully!")


if __name__ == "__main__":
    cassandra_conn = create_cassandra_connection()
    if cassandra_conn:
        create_keyspace(cassandra_conn)
        create_table(cassandra_conn)
        write_stream_data()
        # stop_spark()
    else:
        logger.error("Cassandra connection failed")
        print("Cassandra connection failed")
