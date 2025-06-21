import numpy as np
import pandas as pd
import requests
import os
import time
import json
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from concurrent.futures import ThreadPoolExecutor, as_completed

historical_start = "2024-01-01"
historical_end = "2024-01-07"
future_start = "2050-01-01"
future_end = "2050-01-07"
output_file = r"E:\HazeTransformer\03\data\02weather.csv"
os.makedirs("data", exist_ok=True)
aq = pd.read_csv(r"E:\HazeTransformer\03\data\02pollutant.csv", parse_dates=["Date Local"])
counties = aq[["State Name", "County Name"]].drop_duplicates().reset_index(drop=True)
geolocator = Nominatim(user_agent="county-weather-fetcher")
rate_limited_geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
cache_file = r"E:\HazeTransformer\03\data\latlon_cache.json"
if os.path.exists(cache_file):
    with open(cache_file, "r") as f:
        latlon_cache = json.load(f)
else:
    latlon_cache = {}
def get_lat_lon_cached(state, county, max_retries=3):
    key = f"{county}, {state}"
    if key in latlon_cache:
        return latlon_cache[key]
    for attempt in range(max_retries):
        try:
            loc = rate_limited_geocode(f"{county} County, {state}, USA", timeout=15)
            if loc:
                latlon_cache[key] = (loc.latitude, loc.longitude)
                with open(cache_file, "w") as f:
                    json.dump(latlon_cache, f)
                return latlon_cache[key]
        except Exception as e:
            print(f"⚠️ Retry {attempt+1}/{max_retries} for {key} due to error: {e}")
            time.sleep(2 * (attempt + 1))
    print(f"Skipped geocoding for {key}")
    return None, None
def fetch_weather_for_county(row):
    state, county = row["State Name"], row["County Name"]
    lat, lon = get_lat_lon_cached(state, county)
    if lat is None or lon is None:
        return None

    try:
        url = (
            f"https://archive-api.open-meteo.com/v1/archive?"
            f"latitude={lat}&longitude={lon}"
            f"&start_date={historical_start}&end_date={historical_end}"
            f"&hourly=temperature_2m,wind_speed_10m,relative_humidity_2m"
            f"&timezone=auto"
        )
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()["hourly"]

        df = pd.DataFrame(data)
        df["time"] = pd.to_datetime(df["time"])
        df["State Name"] = state
        df["County Name"] = county
        df["latitude"] = lat
        df["longitude"] = lon
        proj_url_585 = (
            f"https://climate-api.open-meteo.com/v1/climate?"
            f"latitude={lat}&longitude={lon}"
            f"&start_date=2050-01-01&end_date=2050-10-31"
            f"&daily=temperature_2m_min,temperature_2m_max"
            f"&climate_model=INM-CM5-0&ssp=ssp585"
            f"&temperature_unit=celsius&timezone=auto"
        )
        proj_585 = requests.get(proj_url_585).json()
        dates = pd.to_datetime(proj_585["daily"]["time"])
        tmin_585 = proj_585["daily"]["temperature_2m_min"]
        tmax_585 = proj_585["daily"]["temperature_2m_max"]

        ssp585_hourly = []
        for i in range(len(dates)):
            for h in range(24):
                frac = h / 24.0
                temp = tmin_585[i] + (tmax_585[i] - tmin_585[i]) * np.sin(np.pi * frac)
                ssp585_hourly.append(temp)
        proj_url_245 = (
            f"https://climate-api.open-meteo.com/v1/climate?"
            f"latitude={lat}&longitude={lon}"
            f"&start_date=2050-01-01&end_date=2050-10-31"
            f"&daily=temperature_2m_min,temperature_2m_max"
            f"&climate_model=INM-CM5-0&ssp=ssp245"
            f"&temperature_unit=celsius&timezone=auto"
        )
        proj_245 = requests.get(proj_url_245).json()
        tmin_245 = proj_245["daily"]["temperature_2m_min"]
        tmax_245 = proj_245["daily"]["temperature_2m_max"]
        ssp245_hourly = []
        for i in range(len(dates)):
            for h in range(24):
                frac = h / 24.0
                temp = tmin_245[i] + (tmax_245[i] - tmin_245[i]) * np.sin(np.pi * frac)
                ssp245_hourly.append(temp)
        if len(ssp585_hourly) < len(df):
            ssp585_hourly.extend([ssp585_hourly[-1]] * (len(df) - len(ssp585_hourly)))
        elif len(ssp585_hourly) > len(df):
            ssp585_hourly = ssp585_hourly[:len(df)]

        if len(ssp245_hourly) < len(df):
            ssp245_hourly.extend([ssp245_hourly[-1]] * (len(df) - len(ssp245_hourly)))
        elif len(ssp245_hourly) > len(df):
            ssp245_hourly = ssp245_hourly[:len(df)]

        df["temperature_ssp585"] = ssp585_hourly
        df["temperature_ssp245"] = ssp245_hourly

        return df
    except Exception as e:
        print(f"{county}, {state}: {e}")
        return None
print(f"Downloading hourly weather for {len(counties)} counties...\n")
results = []
with ThreadPoolExecutor(max_workers=8) as executor:
    futures = {executor.submit(fetch_weather_for_county, row): row for _, row in counties.iterrows()}
    for i, future in enumerate(as_completed(futures)):
        county_info = futures[future]
        try:
            result = future.result()
            if result is not None:
                results.append(result)
                print(f"({i+1}) {county_info['County Name']}, {county_info['State Name']}")
            else:
                print(f"Skipped ({i+1}) {county_info['County Name']}, {county_info['State Name']}")
        except Exception as e:
            print(f"Error in task: {e}")
if results:
    all_weather = pd.concat(results, ignore_index=True)
    all_weather.to_csv(output_file, index=False)
    print(f"\nSaved hourly weather data to: {output_file}")
else:
    print("No weather data was collected.")
