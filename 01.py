import pandas as pd
import logging
import os
from dateutil import parser
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
os.makedirs("data", exist_ok=True)
pm = pd.read_csv(
    r"E:\HazeTransformer\03\data\daily_88101_2024.csv",
    usecols=["Date Local", "Arithmetic Mean", "State Name", "County Name"],
    low_memory=False
)
ozone = pd.read_csv(
    r"E:\HazeTransformer\03\data\daily_44201_2024.csv",
    usecols=["Date Local", "Arithmetic Mean", "State Name", "County Name"],
    low_memory=False
)
for df in (pm, ozone):
    df["State Name"] = df["State Name"].str.strip()
    df["County Name"] = df["County Name"].str.strip()
def parse_dates(series, label):
    parsed, failed = [], 0
    for val in series:
        try:
            dt = parser.parse(str(val), dayfirst=False)
            parsed.append(dt.date())
        except:
            parsed.append(pd.NaT)
            failed += 1
    logger.info(f"{label}: Parsed {len(parsed)-failed} dates, {failed} failed.")
    return pd.to_datetime(parsed, errors='coerce')
pm["Date Local"] = parse_dates(pm["Date Local"], "PM2.5")
ozone["Date Local"] = parse_dates(ozone["Date Local"], "O3")
start_date = pd.to_datetime("2024-01-01")
end_date = pd.to_datetime("2024-01-07")
pm = pm[(pm["Date Local"] >= start_date) & (pm["Date Local"] <= end_date)]
ozone = ozone[(ozone["Date Local"] >= start_date) & (ozone["Date Local"] <= end_date)]
pm = pm.dropna(subset=["Date Local", "Arithmetic Mean"])
ozone = ozone.dropna(subset=["Date Local", "Arithmetic Mean"])
pm = pm.rename(columns={"Arithmetic Mean": "PM2.5"})
ozone = ozone.rename(columns={"Arithmetic Mean": "O3"})
merged = pd.merge(
    pm,
    ozone,
    on=["Date Local", "State Name", "County Name"],
    how="outer"
)
logger.info(f"Total merged rows: {len(merged)}")
logger.info(f"Unique dates: {merged['Date Local'].nunique()}")
logger.info(f"Unique counties: {merged['County Name'].nunique()}")
output_file = os.path.join(r"E:\HazeTransformer\03\data", "01pollutant.csv")
merged.to_csv(output_file, index=False)
logger.info(f"Saved full merged file to: {output_file}")
