import os
import logging
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from cassandra.cluster import Cluster
from dotenv import load_dotenv

load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)

# Environment variables
cassandra_keyspace = os.environ.get("CASSANDRA_KEYSPACE", "analytics")
daily_data_table_name = os.environ.get("DAILY_SINK_TABLE", "dailydata")

# Date Range variables
start_date = os.environ.get("START_DATE", "2024-04-01")
end_date = os.environ.get("END_DATE", "2024-05-28")

def create_cassandra_connection():
    """Create a connection to the Cassandra cluster."""
    try:
        cluster = Cluster(['cassandra_db'], port=9042)
        session = cluster.connect()
        return session
    except Exception as e:
        logger.error(f"Could not create Cassandra connection due to {e}")
        return None

def create_keyspace(session):
    """Create the Cassandra keyspace if it doesn't exist."""
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {cassandra_keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}};
    """)
    print("Keyspace created successfully!")

def create_table(session):
    """Create the table for daily data if it doesn't exist."""
    session.execute(f"""
    CREATE TABLE IF NOT EXISTS {cassandra_keyspace}.{daily_data_table_name} (
        id TEXT PRIMARY KEY,
        latitude DECIMAL,
        longitude DECIMAL,
        date_time TIMESTAMP,
        generationtime_ms DECIMAL,
        utc_offset_seconds DECIMAL,
        timezone TEXT,
        timezone_abbreviation TEXT,
        elevation DECIMAL,
        weather_value DECIMAL)
    """)
    print("Table created successfully!")

def query_daily_data(session, start_date, end_date):
    """Query daily data from Cassandra within the specified date range."""
    try:
        query = f"""
        SELECT id, latitude, longitude, date_time, weather_value
        FROM {cassandra_keyspace}.{daily_data_table_name}
        WHERE date_time >= '{start_date}' AND date_time <= '{end_date}';
        """
        rows = session.execute(query)

        data = []
        for row in rows:
            data.append(row)

        df = pd.DataFrame(data, columns=['id', 'latitude', 'longitude', 'date_time', 'weather_value'])
        return df

    except Exception as e:
        logger.error(f"Could not query data from Cassandra due to {e}")
        return None

def analyze_weather_data(daily_dataframe):
    """Analyze the weather data by building and evaluating a regression model."""
    # Splitting data into features and target
    X = daily_dataframe.drop(columns=["weather_value", "date_time", "id"])
    y = daily_dataframe["weather_value"]

    # Splitting data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Building regression model
    model = LinearRegression()
    model.fit(X_train, y_train)

    # Predictions
    y_pred = model.predict(X_test)

    # Evaluation
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print(f"Daily Data Model - MSE: {mse}, R2: {r2}")

    # Visualizations
    plt.figure(figsize=(10, 8))
    sns.heatmap(daily_dataframe.corr(), annot=True, cmap='coolwarm')
    plt.title('Heatmap of Variable Correlations')
    plt.show()

    plt.figure(figsize=(10, 8))
    sns.kdeplot(y_test, label='True Values', shade=True)
    sns.kdeplot(y_pred, label='Predicted Values', shade=True)
    plt.title('KDE Plot for Predictions')
    plt.legend()
    plt.show()

    plt.figure(figsize=(10, 8))
    sns.boxplot(data=daily_dataframe[['weather_value']])
    plt.title('Box Plot for Weather Values')
    plt.show()

    plt.figure(figsize=(10, 8))
    plt.scatter(y_test, y_pred, label='Predictions')
    plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'k--', lw=2)
    plt.xlabel('Measured')
    plt.ylabel('Predicted')
    plt.title('Prediction Scatter Plot')
    plt.legend()
    plt.show()

def main():
    # Create a connection to Cassandra
    cassandra_conn = create_cassandra_connection()
    if cassandra_conn:
        create_keyspace(cassandra_conn)
        create_table(cassandra_conn)

        # Query the data
        df = query_daily_data(cassandra_conn, start_date, end_date)

        if df is not None and not df.empty:
            # Analyze and visualize the data
            analyze_weather_data(df)
        else:
            print("No data available for analysis")
    else:
        logger.error("Cassandra connection failed")
        print("Cassandra connection failed")

if __name__ == "__main__":
    main()

