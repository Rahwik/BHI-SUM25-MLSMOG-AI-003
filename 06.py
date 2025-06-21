import os
import json
import pandas as pd
import geopandas as gpd
import osmnx as ox
from shapely.geometry import Point
from tqdm import tqdm

# Load weather dataset with county locations
weather_df = pd.read_csv(r"E:\HazeTransformer\03\data\02weather.csv")
location_df = weather_df.groupby(["State Name", "County Name"])[["latitude", "longitude"]].first().reset_index()

# Output folder for OSM data
output_file = r"E:\HazeTransformer\03\data\03urbanfeatures.csv"
os.makedirs(os.path.dirname(output_file), exist_ok=True)

# Define radius (in meters) around each county center to extract urban features
radius_m = 3000  # 3km buffer
features_list = []

import logging
logging.getLogger("osmnx").setLevel(logging.ERROR)

# Feature types to extract
osm_tags = {
    'building': True,
    'highway': True,
    'landuse': True,
    'leisure': True,
    'amenity': True,
    'shop': True,
    'railway': True,
    'waterway': True,
    'natural': True
}

print(f"\nExtracting OSM urban features for {len(location_df)} counties...\n")

for _, row in tqdm(location_df.iterrows(), total=len(location_df)):
    state, county = row["State Name"], row["County Name"]
    lat, lon = row["latitude"], row["longitude"]
    
    try:
        # Create a circular buffer around the county center
        gdf = ox.features_from_point((lat, lon), tags=osm_tags, dist=radius_m) # type: ignore

        # Count number of features per type
        summary = {
            "State Name": state,
            "County Name": county,
            "latitude": lat,
            "longitude": lon,
            "num_buildings": gdf[gdf["building"].notnull()].shape[0],
            "num_roads": gdf[gdf["highway"].notnull()].shape[0],
            "num_landuse": gdf[gdf["landuse"].notnull()].shape[0],
            "num_amenities": gdf[gdf["amenity"].notnull()].shape[0],
            "num_shops": gdf[gdf["shop"].notnull()].shape[0],
            "num_leisure": gdf[gdf["leisure"].notnull()].shape[0],
            "num_railways": gdf[gdf["railway"].notnull()].shape[0],
            "num_waterways": gdf[gdf["waterway"].notnull()].shape[0],
            "num_natural": gdf[gdf["natural"].notnull()].shape[0]
        }

        features_list.append(summary)

    except Exception as e:
        print(f"⚠️ Skipped {county}, {state}: {e}")

# Save to CSV
features_df = pd.DataFrame(features_list)
features_df.to_csv(output_file, index=False)

print(f"\n✅ Saved OSM urban features to: {output_file}")
