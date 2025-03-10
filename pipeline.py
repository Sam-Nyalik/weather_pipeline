import requests
import pandas as pd
import sqlite3
from decouple import config

# Extract data from the API
def extract_weather_data(cities, api_key):
    # Fetch weather data for a list of cities
    url = "https://api.openweathermap.org/data/2.5/weather"
    weather_data = []
    
    for city in cities:
        params = {"q": city, "appid": api_key, "units": "metric"}
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            weather_data.append(response.json())
        else:
            print(f"Failed to fetch data for {city}: {response.status_code}")
    
    return weather_data

# Transform data
def transform_weather_data(data):
    # Transforming raw weather data into a structured format
    transformed_data = []
    
    for item in data:
        transformed_data.append({
            "city": item["name"],
            "temperature": item["main"]["temp"],
            "humidity": item["main"]["humidity"],
            "weather": item["weather"][0]["description"],
        })
    
    return pd.DataFrame(transformed_data)

# Load the data into SQLite
def load_data_to_db(data, db_path, table_name):
    conn = sqlite3.connect(db_path)
    data.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    
    
# Main Pipeline
def main():
    # Define our API Keys and cities
    api_key = config("OPENWEATHER_API_KEY")
    cities = ["Nairobi", "Kisumu"]
    
    # File paths
    db_path = "data/weather.db"
    table_name = "weather"
    
    # ETL Steps
    raw_data = extract_weather_data(cities, api_key)
    clean_data = transform_weather_data(raw_data)
    load_data_to_db(clean_data, db_path, table_name)
    
    print("Data pipeline has been executed successfully!")

if __name__ == "__main__":
    main()