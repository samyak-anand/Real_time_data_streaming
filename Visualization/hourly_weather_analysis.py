import os
from cassandra.cluster import Cluster
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from airflow.models import Variable
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration for Cassandra
cassandra_keyspace = 'analytics'
hourly_data_table_name = 'hourlydata'

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

def get_request_parameters(start_date, end_date) -> dict:
    """Create a dictionary of the request parameters for the queries"""
    try:
        logger.info("Variables returned successfully")
        return {
            "latitude": latitudes,
            "longitude": longitudes,
            "hourly": hourly,
            "daily": daily,
            "start_date": start_date,
            "end_date": end_date
        }
    except Exception as var_except:
        logger.error(var_except.__str__())
        return False

def query_temperature_data(start_date, end_date):
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['cassandra_db'], port=9042)
        session = cluster.connect(cassandra_keyspace)

        query = f"""
        SELECT id, latitude, longitude, date_time, temperature_2m, relative_humidity_2m, dew_point_2m, apparent_temperature, rain, surface_pressure, temperature_80m 
        FROM {cassandra_keyspace}.{hourly_data_table_name}
        WHERE date_time >= '{start_date}' AND date_time <= '{end_date}';
        """
        rows = session.execute(query)

        data = []
        for row in rows:
            data.append(row)

        df = pd.DataFrame(data, columns=['id', 'latitude', 'longitude', 'date_time', 'temperature_2m', 'relative_humidity_2m', 'dew_point_2m', 'apparent_temperature', 'rain', 'surface_pressure', 'temperature_80m'])
        return df

    except Exception as e:
        logger.error(f"Could not query data from Cassandra due to {e}")
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

def analyze_weather_data(hourly_dataframe):
    # Splitting data into features and target
    X = hourly_dataframe.drop(columns=["temperature_2m", "rain", "date_time"])
    y_temp = hourly_dataframe["temperature_2m"]
    y_rain = hourly_dataframe["rain"]

    # Splitting data into training and testing sets
    X_train, X_test, y_temp_train, y_temp_test, y_rain_train, y_rain_test = train_test_split(X, y_temp, y_rain, test_size=0.2, random_state=42)

    # Building regression models
    temp_model = LinearRegression()
    rain_model = LinearRegression()

    temp_model.fit(X_train, y_temp_train)
    rain_model.fit(X_train, y_rain_train)

    # Predictions
    y_temp_pred = temp_model.predict(X_test)
    y_rain_pred = rain_model.predict(X_test)

    # Evaluation
    temp_mse = mean_squared_error(y_temp_test, y_temp_pred)
    temp_r2 = r2_score(y_temp_test, y_temp_pred)

    rain_mse = mean_squared_error(y_rain_test, y_rain_pred)
    rain_r2 = r2_score(y_rain_test, y_rain_pred)

    print(f"Temperature Model - MSE: {temp_mse}, R2: {temp_r2}")
    print(f"Rain Model - MSE: {rain_mse}, R2: {rain_r2}")

    # Visualizations
    # Heatmap of correlations
    plt.figure(figsize=(10, 8))
    sns.heatmap(hourly_dataframe.corr(), annot=True, cmap='coolwarm')
    plt.title('Heatmap of Variable Correlations')
    plt.show()

    # KDE Plot for Temperature Predictions
    plt.figure(figsize=(10, 8))
    sns.kdeplot(y_temp_test, label='True Temperature', shade=True)
    sns.kdeplot(y_temp_pred, label='Predicted Temperature', shade=True)
    plt.title('KDE Plot for Temperature Predictions')
    plt.legend()
    plt.show()

    # Box Plot for Temperature and Rain
    plt.figure(figsize=(10, 8))
    sns.boxplot(data=hourly_dataframe[['temperature_2m', 'rain']])
    plt.title('Box Plot for Temperature and Rain')
    plt.show()

    # Scatter Plot for Temperature Predictions
    plt.figure(figsize=(10, 8))
    plt.scatter(y_temp_test, y_temp_pred, label='Temperature Predictions')
    plt.plot([y_temp_test.min(), y_temp_test.max()], [y_temp_test.min(), y_temp_test.max()], 'k--', lw=2)
    plt.xlabel('Measured')
    plt.ylabel('Predicted')
    plt.title('Temperature Prediction Scatter Plot')
    plt.legend()
    plt.show()

    # Scatter Plot for Rain Predictions
    plt.figure(figsize=(10, 8))
    plt.scatter(y_rain_test, y_rain_pred, label='Rain Predictions')
    plt.plot([y_rain_test.min(), y_rain_test.max()], [y_rain_test.min(), y_rain_test.max()], 'k--', lw=2)
    plt.xlabel('Measured')
    plt.ylabel('Predicted')
    plt.title('Rain Prediction Scatter Plot')
    plt.legend()
    plt.show()

    # Additional descriptive statistics
    print(hourly_dataframe.describe())

def main():
    # Get request parameters
    params = get_request_parameters(start_date, end_date)

    # Query the data
    df = query_temperature_data(params["start_date"], params["end_date"])

    if df is not None and not df.empty:
        # Analyze and visualize the data
        analyze_temperature_data(df)
        # Build regression models and visualize
        analyze_weather_data(df)
    else:
        print("No data available for analysis")

if __name__ == "__main__":
    main()
