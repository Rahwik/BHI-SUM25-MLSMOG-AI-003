import pandas as pd
import numpy as np
from scipy.interpolate import griddata
import os

# Load weather + projection data
weather_df = pd.read_csv(r"E:\HazeTransformer\03\data\02weather.csv", parse_dates=["time"])

# Group daily values per county
group_keys = ["State Name", "County Name", weather_df["time"].dt.date]
daily_data = weather_df.groupby(group_keys).agg({
    "temperature_ssp245": "mean",
    "temperature_ssp585": "mean"
}).reset_index()
daily_data = daily_data.rename(columns={"time": "date"})

# Merge with county lat/lon
location_lookup = weather_df.groupby(["State Name", "County Name"])[["latitude", "longitude"]].first().reset_index()
daily_data = daily_data.merge(location_lookup, on=["State Name", "County Name"])

# Output folder
output_dir = r"D:\HazeTransformer\03\data\cmip6_daily_grids"
os.makedirs(output_dir, exist_ok=True)

# Create 1 km grid over bounding box
grid_spacing = 0.01  # ~1 km
lat_min, lat_max = daily_data["latitude"].min(), daily_data["latitude"].max()
lon_min, lon_max = daily_data["longitude"].min(), daily_data["longitude"].max()
lat_grid = np.arange(lat_min, lat_max + grid_spacing, grid_spacing)
lon_grid = np.arange(lon_min, lon_max + grid_spacing, grid_spacing)
grid_lon, grid_lat = np.meshgrid(lon_grid, lat_grid)
grid_points = np.column_stack([grid_lat.ravel(), grid_lon.ravel()])

# Interpolate each date and save as compressed Parquet with float32
for date in daily_data["date"].unique():
    day_df = daily_data[daily_data["date"] == date]
    points = day_df[["latitude", "longitude"]].values

    interpolated = {
        "latitude": grid_points[:, 0].astype(np.float32),
        "longitude": grid_points[:, 1].astype(np.float32),
        "date": pd.to_datetime(date)
    }

    for var in ["temperature_ssp245", "temperature_ssp585"]:
        values = day_df[var].values.astype(np.float32)
        interp_linear = griddata(points, values, grid_points, method="linear")
        interp_nearest = griddata(points, values, grid_points, method="nearest")
        interp_combined = np.where(np.isnan(interp_linear), interp_nearest, interp_linear)
        interpolated[var] = interp_combined.astype(np.float32)

    df = pd.DataFrame(interpolated).dropna(subset=["temperature_ssp245", "temperature_ssp585"])
    date_str = pd.to_datetime(date).strftime("%Y-%m-%d")
    df.to_parquet(os.path.join(output_dir, f"downscaled_{date_str}.parquet"), index=False)
    print(f"Saved: downscaled_{date_str}.parquet")

print(f"\nAll CMIP6 downscaled daily grids saved to: {output_dir}")
