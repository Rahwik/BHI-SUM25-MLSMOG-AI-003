import os
import requests
from datetime import datetime, timedelta
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from bs4.element import Tag
from tqdm import tqdm
import time

# Configuration
product = "MOD04_L2"
collection = "61"
start_date = datetime.strptime("2024-01-01", "%Y-%m-%d")
end_date = datetime.strptime("2024-01-07", "%Y-%m-%d")
output_dir = r"E:\HazeTransformer\03\data\modis_aod_raw"
os.makedirs(output_dir, exist_ok=True)

def list_hdf_files(product: str, collection: str, date_obj: datetime) -> list[str]:
    yyyy = date_obj.strftime("%Y")
    jjj = date_obj.strftime("%j")
    base_url = f"https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/{collection}/{product}/{yyyy}/{jjj}/"

    try:
        response = requests.get(base_url, timeout=15)
        if response.status_code != 200:
            print(f"Failed to access {base_url}: Status code {response.status_code}")
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        files: list[str] = []

        for node in soup.find_all("a"):
            if isinstance(node, Tag):
                href = node.get("href")
                if isinstance(href, str) and href.endswith(".hdf"):
                    files.append(urljoin(base_url, href))
        return files

    except Exception as e:
        print(f"Error retrieving files from {base_url}: {e}")
        return []

def download_file(url: str, output_dir: str) -> None:
    filename = url.split("/")[-1]
    path = os.path.join(output_dir, filename)
    if os.path.exists(path):
        return
    try:
        with requests.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            with open(path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except Exception as e:
        print(f"Failed to download {url}: {e}")

# Iterate through dates
date = start_date
all_urls: list[str] = []

while date <= end_date:
    urls = list_hdf_files(product, collection, date)
    all_urls.extend(urls)
    time.sleep(1)  # Prevent rate limiting
    date += timedelta(days=1)

# Download files
for url in tqdm(all_urls, desc="Downloading MODIS AOD files"):
    download_file(url, output_dir)
