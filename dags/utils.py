"""
All generic functions to be reusable by multiple modules
"""
# Import the neccessary packages
import os
import requests
import json
import logging
# from datetime import datetime
from uuid import uuid4

# Third-Party packages
from dotenv import load_dotenv

from airflow.models import Variable

# Define variables
load_dotenv()

logger = logging.getLogger(__name__)

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
start_date = Variable.get("start_date", "2024-05-01")
end_date = Variable.get("end_date", "2024-05-28")


def get_request_paramters(start_date, end_date) -> dict:
    """Create a dictionary of the request parameters for the queries"""
    try:
        logger.info("Variables returned suffessfully")
        return {
            "latitude": latitudes,
            "longitude": longitudes,
            "hourly": hourly,
            "daily": daily,
            "start_date": start_date,
            "end_date": end_date
        }
    except Exception as var_except:
        print(var_except.__str__())
        # logger.error(var_except.__str__())
        return False


def get_hourly_data(start_date, end_date):
    """Get hourly data from the API endpoint"""
    try:
        request_parameters = get_request_paramters(start_date, end_date)
        # Remove the daily parameters to only get hourly parameters
        request_parameters.pop("daily")
        print(request_parameters)
        # Construct the query parameters
        formatted_params = "&".join([k+'='+str(v) for k, v in request_parameters.items()])
        # Contruct the full URL with the query params
        url = f"{base_url}?{formatted_params}"
        print(url)
        headers = {}

        # Make the request to the endpoint for a response data
        response = requests.get(url, headers=headers)

        print(response.text)
        # Check for request success
        if response.status_code == 200:
            logger.info("Request executed successfully with a valid response")
            return response.json()
        else:
            logger.info(f"Failed Request: StatusCode: {response.status_code}. Error: {response.text}")
            return False
    except Exception as hourly_data_except:
        logger.error(hourly_data_except.__str__())
        return False


def get_daily_data(start_date, end_date):
    """Get the daily data from the API Endpoint"""
    try:
        request_parameters = get_request_paramters(start_date, end_date)
        print(request_parameters)
        if request_parameters:
            # Remove the hourly parameters to only get daily parameters
            request_parameters.pop("hourly")
            # Construct the query parameters
            formatted_params = "&".join([k+'='+str(v) for k, v in request_parameters.items()])
            # Contruct the full URL with the query params
            url = f"{base_url}?{formatted_params}"
            print(url)
            headers = {}

            # Make the request to the endpoint for a response data
            response = requests.get(url, headers=headers)
            print(response.text)
            if response.status_code == 200:
                constant_pair = response.json()
                return constant_pair
            else:
                logger.info(f"Failed Request: StatusCode: {response.status_code}. Error: {response.text}")
                return False
    except Exception as daily_data_except:
        logger.error(daily_data_except.__str__())
        return False


def transform_data(time_values, data_values, data_key="weather_value") -> list:
    """Generic data transformation in an ingestible format for kafka and spark"""
    try:
        time_data = time_values
        weather_data = data_values
        result = [{"id": str(uuid4()), "date_time": date, data_key: value} for date, value in zip(time_data, weather_data)]

        return result
    except Exception as transform_except:
        logger.error(transform_except.__str__())
        return False


def daily_main(start_date=start_date, end_date=end_date):
    """Process the daily data for loading"""
    response_data = get_daily_data(start_date, end_date)
    # data_keys = response_data["daily_units"].keys()
    # items_to_remove = ("daily_units", "daily")
    i = 0
    for item in response_data:
        data_values = item.pop("daily")
        item.pop("daily_units")

        data_dict = transform_data(data_values["time"], data_values["weather_code"])

        for new_item in data_dict:
            i += 1
            new_item.update(item)
        print(json.dumps(data_dict))
        yield (json.dumps(i), json.dumps(data_dict))


def hourly_main(start_date=start_date, end_date=end_date):
    """Process the hourly data coming in. """
    response_data = get_hourly_data(start_date, end_date)
    # lat_long_map = list(zip(latitudes.split(","), longitudes.split(",")))

    i = 0
    for item in response_data:
        hourly_units = item.pop("hourly_units", None)
        hourly_units.pop("time")
        # default_map = dict.fromkeys(hourly_units, 0)
        data_values = item.pop("hourly", None)
        time_data = data_values.pop("time")
        # item.update(default_map)
        for hourly_key, hourly_val in data_values.items():
            data_array = transform_data(time_data, hourly_val, hourly_key)
            for each_data in data_array:
                i += 1
                each_data.update(item)
            json_str = json.dumps(data_array)
            yield (json.dumps(i), json_str)
