"""
Spark Stream processing Hourly - Consuming from Kafka
Loading to Cassadra
"""
# Import necessary packages
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import (
    StringType, StructType,
    DecimalType, TimestampType, ArrayType,
)

from dotenv import load_dotenv

from cassandra.cluster import Cluster

load_dotenv()

# Declare needed variables
logger = logging.getLogger(__name__)
hourly_topic = os.environ.get("HOURLY_DATA_TOPIC", "hourlymetrics")
hourly_data_table_name = os.environ.get("HOURLY_SINK_TABLE", "hourlydata")
cassandra_keyspace = os.environ.get("CASSANDRA_KEYSPACE", "analytics")
bootstrap_server = "kafka:9092"

# Schema for the hourly table in cassandra. This can be changed based on the hourly metrics needed
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
    .add("temperature_2m", DecimalType(scale=4))
    .add("relative_humidity_2m", DecimalType(scale=4))
    .add("dew_point_2m", DecimalType(scale=4))
    .add("apparent_temperature", DecimalType(scale=4))
    .add("rain", DecimalType(scale=4))
    .add("surface_pressure", DecimalType(scale=4))
    .add("temperature_80m", DecimalType(scale=4))
    .add("precipitation", DecimalType(scale=4))
)

# Convert schema to array due to structure of data being produced in Kafka
# Use the sample_schema (StructType) if it's a single json data
array_schema = ArrayType(sample_schema)


def spark_connect():
    """Create the spark connection to be used for other interaction and processing"""
    spark = (
        SparkSession.builder.appName("WeatherAppHourly")
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
    Define the subscription to kafka topic and read stream
    In reading, conert the json string to a string and explode
    """
    df = spark_session \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_server) \
        .option("subscribe", hourly_topic) \
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
        .option("table", hourly_data_table_name)\
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
    CREATE TABLE IF NOT EXISTS {cassandra_keyspace}.{hourly_data_table_name} (
        id TEXT PRIMARY KEY,
        latitude DECIMAL,
        longitude DECIMAL,
        date_time DATE,
        generationtime_ms DECIMAL,
        utc_offset_seconds DECIMAL,
        timezone TEXT,
        timezone_abbreviation TEXT,
        elevation DECIMAL,
        temperature_2m DECIMAL,
        relative_humidity_2m DECIMAL,
        dew_point_2m DECIMAL,
        apparent_temperature DECIMAL,
        rain DECIMAL,
        surface_pressure DECIMAL,
        temperature_80m DECIMAL,
        precipitation DECIMAL)
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
