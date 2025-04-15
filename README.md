# Citi Bike Data Pipeline (2019-2020)
This repository contains the final project for the [January–April 2025 DE Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp). It implements a data pipeline to extract, transform, load and visualize Citi Bike trip data for both New York City and Jersey City for the years **2019** and **2020**.

---

## Introduction

Citi Bike is New York City and Jersey City's bike share system and one of the largest and most popular in the United States. Launched in May 2013, Citi Bike quickly became an integral part of the cities’ transportation infrastructure. By 2019 and 2020, the system saw significant expansion, with thousands of new docking stations added across various neighborhoods. These two years marked a period of rapid growth and changing ridership habits, shaped by new ways people chose to get around the city and the sudden impact of the COVID-19 pandemic in 2020.

Analyzing Citi Bike data from this period provides valuable insights into how people moved through the city, how ridership fluctuated across seasons, how station usage evolved, and how different age groups and customer types engaged with the service.

---

## Pipeline Overview
The pipeline covers the following steps:

1. **Data Extraction and Transformation**
2. **Batch processing to load data into Google Cloud Storage using Spark**
3. **Data moving from GCS to BigQuery using Kestra**
4. **Data Visualization including toggleable heatmap layers and interactive dashboard using Metabase**

---

## Step-by-Step Pipeline Description

### 1. **Data Extraction**
- **Data Source:** Citi Bike trip history data is downloaded from [citybikenyc.com](https://citibikenyc.com/system-data).
- **Why 2019 and 2020?**  
The years 2019 and 2020 were selected because, starting in January 2021 (and February 2021 for Jersey City), the datasets no longer include key demographic variables such as year of birth. Including these years allows for demographic distribution analysis in the results. In addition, Citi Bike significantly expanded its coverage during this period, adding more stations across Brooklyn and Queens in 2019, and extending into the Bronx and Upper Manhattan in 2020—partly in response to increased demand during the COVID-19 pandemic.
- **Unpacking and Recursive Unzipping:**  
After downloading the annual zip folders, each file is unzipped to extract the monthly trip data. Some of these folders may contain embedded zip files, which also need to be unzipped. A recursive unzipping function is implemented to ensure that all nested zip files are fully extracted for downstream processing.




### 2. **Data Extraction and Transformation**
- **Unzipping:** Files are recursively unzipped and cleaned from extra nested zips and metadata.
- **Spark Setup:** Apache Spark is used for scalable data processing.
- **Schema Handling:**
  - `schema_old`: Applied to 2019 and pre-2020 data.
  - `schema_new`: Applied to NYC data post-2020.
- **Merging Datasets:** NYC and JC datasets are separately read and unioned with `allowMissingColumns=True`.
- **Column Normalization:** Renaming and dropping unneeded columns.
- **Filtering:** Trips are filtered to stay within the NYC and JC bounding box.
- **New Columns:** Year and month are extracted for partitioning and aggregation.

### 3. **Data Storage in GCS**
- **Format:** Data is stored in Parquet format.
- **Destination:** `gs://zoomcamp_final_project/citibike/<year>/`
- **Tool Used:** Apache Spark GCS Connector for write operations.

### 4. **Load into BigQuery**
- **Tool Used:** [Kestra](https://kestra.io/), an open-source orchestration tool.
- **Steps in Kestra:**
  - Create external table referencing GCS parquet.
  - Create a BigQuery table with unique IDs using `MD5` hash of key fields.
  - Merge new rows into final partitioned BigQuery table.
- **Partitioning:** BigQuery is partitioned using `RANGE_BUCKET` on `year` for performance.

### 5. **Visualization**
- **Tool Used:** Folium with Leaflet.js rendering.
- **Type:** Monthly heatmaps overlayed as map layers.
- **Normalization:** Rides are globally normalized for consistent color intensity across months.
- **Output:** An interactive HTML file per year with toggle control between months.
- 
![IMG_0302](https://github.com/user-attachments/assets/7ea3d4b9-0b0b-4938-8cbd-0641f4e1634f)

### Heatmap example of Citi Bike usage in Jan 2019
<img width="925" alt="Screenshot 2025-04-14 at 10 21 50 PM" src="https://github.com/user-attachments/assets/6bce8fa5-4655-44c1-a69b-c8d41e624613" />

Right click 'Save Link As' to download the heat map
https://raw.githubusercontent.com/carolinelile/DE_Zoomcamp_Final_Project/refs/heads/main/citibike_2019_monthly_heatmap_toggle.html
https://raw.githubusercontent.com/carolinelile/DE_Zoomcamp_Final_Project/refs/heads/main/citibike_2020_monthly_heatmap_toggle.html




---

## How to Run
1. Ensure valid Google Cloud credentials are available at:
   ```
   /home/<user>/.google/credentials/service-account.json
   ```
2. Install required libraries:
   ```bash
   pip install pyspark folium google-cloud-storage pandas
   ```
3. Run the script:
   ```bash
   python citibike_etl.py
   ```

---

## Discussion & Areas for Improvement



---

## Author
Caroline Li  
Created as part of the **Data Engineering Zoomcamp Final Project**

