import requests
from datetime import datetime, timedelta
import time
import json
import boto3

OWM_API_KEY = "8be8123961bf1e8135acf22a36c65d8b"
KINESIS_STREAM_NAME = "weather-data-stream"

# Initialize Kinesis client
kinesis_client = boto3.client("kinesis", region_name="us-east-1")

# Logging function
def log_message(message):
    current_time_utc = datetime.utcnow()
    current_time_local = current_time_utc + timedelta(hours=7)
    formatted_time = current_time_local.strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{formatted_time}] {message}")
    

# Get coordinates for a city
def get_long_lat(city):
    try:
        url = f"https://api.openweathermap.org/geo/1.0/direct?q={city}&limit=1&appid={OWM_API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if data:
            return data[0].get("lat"), data[0].get("lon")
        else:
            log_message(f"No data found for {city}.")
            return None, None
        
    except requests.exceptions.RequestException as e:
        log_message(f"Error getting long/lat for {city}: {e}")
        return None, None


# Get daily weather forecast for a city
def get_daily_weather(city):
    lat, lon = get_long_lat(city)
    if lat is None or lon is None:
        return None

    try:
        url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude=current,minutely,hourly,alerts&appid={OWM_API_KEY}&units=metric"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        daily_data = data.get("daily", [])
        formatted_data = []
        for day in daily_data:
            date = datetime.utcfromtimestamp(day.get("dt")).strftime('%Y-%m-%d')
            temp_avg = (day["temp"]["day"] + day["temp"]["night"]) / 2
            temp_min = day["temp"]["min"]
            temp_max = day["temp"]["max"]
            precipitation = day.get("rain", 0)
            snow_depth = day.get("snow", 0)
            wind_dir = day["wind_deg"]
            wind_speed = day["wind_speed"] * 3.6
            pressure = day["pressure"]

            formatted_data.append({
                "time": date,
                "city": city,
                "tavg": round(temp_avg, 2),
                "tmin": temp_min,
                "tmax": temp_max,
                "prcp": precipitation,
                "snow": snow_depth,
                "wdir": wind_dir,
                "wspd": round(wind_speed, 2),
                "pres": pressure,
                "airport_id": "LAX"
            })

        return formatted_data
        
    except requests.exceptions.RequestException as e:
        log_message(f"Error getting weather for {city}: {e}")
        return None
    

# Send data to Kinesis
def put_records_to_kinesis(stream_name, records):
    global sent_dates
    for record in records:
        if record["time"] in sent_dates:
            log_message(f"Record for {record['time']} already sent. Skipping...")
            continue

        try:
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(record),
                PartitionKey=record["airport_id"]
            )
            log_message(f"Record sent to Kinesis. SequenceNumber: {response['SequenceNumber']}")
            sent_dates.add(record["time"])
        except Exception as e:
            log_message(f"Failed to send record to Kinesis: {e}")


# Main logic
if __name__ == "__main__":
    city = "Los Angeles"

    while True:
        # Fetch and send daily weather data
        daily_weather = get_daily_weather(city=city)
        if daily_weather:
            log_message(f"Sending daily weather data for {city} to Kinesis...")
            put_records_to_kinesis(KINESIS_STREAM_NAME, daily_weather)
            log_message(f"All records for {city} have been sent to Kinesis.")
        else:
            log_message(f"No data found for {city}.")

        # Wait for 24 hours before the next execution
        log_message("Waiting for the next day to fetch data...")
        time.sleep(24 * 60 * 60)
