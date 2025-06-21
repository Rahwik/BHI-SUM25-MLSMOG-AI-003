import pandas as pd
import os
import warnings
input_path = r"E:\HazeTransformer\03\data\01pollutant.csv"
output_path = r"E:\HazeTransformer\03\data\02pollutant.csv"
df = pd.read_csv(input_path, parse_dates=["Date Local"])
pm25_missing_before = df["PM2.5"].isna().sum()
o3_missing_before = df["O3"].isna().sum()
print(f"Missing before filtering:")
print(f"   → PM2.5: {pm25_missing_before}")
print(f"   → O3   : {o3_missing_before}")
grouped = df.groupby(["State Name", "County Name"])
missing_ratios = grouped[["PM2.5", "O3"]].apply(lambda g: g.isna().mean())
valid_counties = missing_ratios[(missing_ratios["PM2.5"] < 0.5) & (missing_ratios["O3"] < 0.5)].index
all_counties = set(missing_ratios.index)
dropped_counties = all_counties - set(valid_counties)
print(f"\nDropped {len(dropped_counties)} counties with >50% missing data")
df = df.set_index(["State Name", "County Name"])
df = df.loc[valid_counties].reset_index()
df = df.sort_values(by=["State Name", "County Name", "Date Local"])
with warnings.catch_warnings():
    warnings.simplefilter("ignore", category=RuntimeWarning)
    df["PM2.5"] = df.groupby(["State Name", "County Name"])["PM2.5"].transform(lambda x: x.fillna(x.median()))
    df["O3"] = df.groupby(["State Name", "County Name"])["O3"].transform(lambda x: x.fillna(x.median()))
df["PM2.5"] = df.groupby(["State Name", "County Name"])["PM2.5"].ffill()
df["O3"] = df.groupby(["State Name", "County Name"])["O3"].ffill()
df["PM2.5"] = df["PM2.5"].fillna(-1)
df["O3"] = df["O3"].fillna(-1)
pm25_missing_after = df["PM2.5"].isna().sum()
o3_missing_after = df["O3"].isna().sum()
print(f"\nMissing after filling:")
print(f"   → PM2.5: {pm25_missing_after}")
print(f"   → O3   : {o3_missing_after}")
os.makedirs("data", exist_ok=True)
df.to_csv(output_path, index=False)
print(f"\nCleaned dataset saved to: {output_path}")
