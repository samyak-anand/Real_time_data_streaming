"""
Connects to a Cassandra database and retrieves temperature data.
Converts the data into a Pandas DataFrame.
Analyzes the temperature data by resampling it to hourly averages.
Visualizes the temperature data over time for each geographical location.
"""

from cassandra.cluster import Cluster
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import logging
import pandas as pd
from cassandra.cluster import Cluster
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import mean_squared_error, mean_absolute_error
from pyspark.sql import SparkSession

from dags.spark_stream_hourly import cassandra_keyspace, hourly_data_table_name, logger


def query_temperature_data():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['cassandra_db'], port=9042)
        session = cluster.connect(cassandra_keyspace)

        query = f"SELECT id, latitude, longitude, date_time, temperature_2m FROM {cassandra_keyspace}.{hourly_data_table_name};"
        rows = session.execute(query)

        data = []
        for row in rows:
            data.append(row)

        df = pd.DataFrame(data, columns=['id', 'latitude', 'longitude', 'date_time', 'temperature_2m'])
        return df

    except Exception as e:
        logger.error(f"Could not query data from cassandra due to {e}")
        return None


def analyze_temperature_data(df):
    # Convert date_time to datetime type
    df['date_time'] = pd.to_datetime(df['date_time'])

    # Set the index to date_time for resampling
    df.set_index('date_time', inplace=True)

    # Group by location (latitude, longitude) and resample the data to hourly means
    grouped = df.groupby(['latitude', 'longitude']).resample('H').mean().reset_index()

    # Plot the temperature data for each location
    locations = grouped[['latitude', 'longitude']].drop_duplicates()

    for _, location in locations.iterrows():
        lat, lon = location['latitude'], location['longitude']
        location_data = grouped[(grouped['latitude'] == lat) & (grouped['longitude'] == lon)]

        plt.figure(figsize=(10, 6))
        plt.plot(location_data['date_time'], location_data['temperature_2m'], label=f'Location ({lat}, {lon})')
        plt.title(f'Temperature over Time at Location ({lat}, {lon})')
        plt.xlabel('Time')
        plt.ylabel('Temperature (°C)')
        plt.legend()
        plt.grid(True)
        plt.show()


def main():
    # Query the data
    df = query_temperature_data()

    if df is not None and not df.empty:
        # Analyze and visualize the data
        analyze_temperature_data(df)
    else:
        print("No data available for analysis")


if __name__ == "__main__":
    main()
