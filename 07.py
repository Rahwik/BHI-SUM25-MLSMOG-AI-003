import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import box
import os

weather_df = pd.read_csv(r"E:\HazeTransformer\03\data\02weather.csv")

lat_min = weather_df["latitude"].min()
lat_max = weather_df["latitude"].max()
lon_min = weather_df["longitude"].min()
lon_max = weather_df["longitude"].max()

grid_size = 0.01  # ~1km

lat_points = np.arange(lat_min, lat_max + grid_size, grid_size)
lon_points = np.arange(lon_min, lon_max + grid_size, grid_size)

output_file = r"E:\HazeTransformer\03\data\grid_1km.geojson"
os.makedirs(os.path.dirname(output_file), exist_ok=True)

# Use iterator pattern for large area
def generate_grid():
    for i, lat in enumerate(lat_points):
        for j, lon in enumerate(lon_points):
            yield {
                "grid_id": f"grid_{i}_{j}",
                "geometry": box(lon, lat, lon + grid_size, lat + grid_size)
            }

# Process and write in one batch to avoid GeoJSON append issues
features = list(generate_grid())
gdf = gpd.GeoDataFrame(features, crs="EPSG:4326")
gdf.to_file(output_file, driver="GeoJSON")

print(f"Saved 1km x 1km spatial grids to: {output_file}")
