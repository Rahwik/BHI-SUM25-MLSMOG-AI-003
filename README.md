# BHI-SUM25-MLSMOG-AI-003\

This repository contains a complete pipeline for a **Transformer Implementation** focused on air quality modeling and climate impact analysis. The project gathers, cleans, and integrates multiple data sources to construct a rich, spatio-temporal dataset ready for advanced modeling.

The final dataset is built from a diverse range of features, including:
- **Surface Pollutants:** Ground-level measurements of PM2.5 and Ozone.
- **Meteorological Data:** Historical weather data and future climate projections (SSP245, SSP585).
- **Satellite Imagery:** MODIS Aerosol Optical Depth (AOD) data.
- **Urban & Geographical Features:** Data on the built and natural environment from OpenStreetMap.

## Feature Engineering Pipeline

The data processing pipeline is a series of automated steps that builds the final dataset for the model.

1.  **Pollutant Data Processing:** Raw ground-level pollutant data (PM2.5, Ozone) is cleaned, normalized, and merged. The process handles missing values through robust imputation techniques to ensure data quality.

2.  **Geospatial Feature Integration:** The pipeline enriches the pollutant data with critical geospatial features.
    - **Weather & Climate:** It fetches historical meteorological data and future climate projections from external APIs for each location.
    - **Satellite Data:** Aerosol Optical Depth (AOD) data is downloaded from NASA's satellite archives to provide an atmospheric perspective on air quality.
    - **Urban Density:** Urban features (buildings, roads, land use) are extracted from OpenStreetMap to quantify the urban environment's characteristics.

3.  **High-Resolution Grid Generation:** All data is mapped onto a high-resolution (1km x 1km) spatial grid. Climate and weather data are downscaled using spatial interpolation to provide granular, localized features for the model.

## Installation

To get started, you will need Python 3.

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2.  **Create and activate a virtual environment:**
    ```bash
    python -m venv venv
    # On Windows
    .\venv\Scripts\activate
    # On macOS/Linux
    source venv/bin/activate
    ```

3.  **Install the required packages:**
    ```bash
    pip install -r requirements.txt
    ```

## Usage

Before running the pipeline, ensure you have the initial input data and directory structure in place.

1.  **Prepare the data directory:**
    - Create a `data/` directory.
    - Place the initial raw pollutant data files in the `data/` directory.

2.  **Execute the pipeline:**
    Run the data processing scripts to generate the final dataset. The scripts are designed to be executed sequentially to ensure dependencies are met.
