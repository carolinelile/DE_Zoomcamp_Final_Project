# Citi Bike Data Pipeline (2019-2020)
The final project of 2025 DE Zoomcamp, it implements a data pipeline to extract, transform, load and visualize Citi Bike trip data for both New York City and Jersey City for the years **2019** and **2020**.

---

## Overview
The pipeline covers the following steps:

1. **Data Ingestion**
2. **Data Extraction and Transformation**
3. **Data Storage in Google Cloud Storage (GCS)**
4. **Data Load into BigQuery (via Kestra)**
5. **Visualization**

---

## Step-by-Step Pipeline Description

### 1. **Data Ingestion**
- **Source:** Citi Bike trip data is downloaded from [s3.amazonaws.com/tripdata](https://s3.amazonaws.com/tripdata/).
- **NYC Data:** Provided in yearly zipped files.
- **Jersey City (JC) Data:** Provided monthly with `JC-YYYYMM` prefix.
- **Tool Used:** Python `requests` library to fetch data.

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
![IMG_0298](https://github.com/user-attachments/assets/b56ceb79-6ef6-436f-a434-c22885b70c4a)

### Heatmap example of Citi Bike usage in Jan 2019
<img width="925" alt="Screenshot 2025-04-14 at 10 21 50 PM" src="https://github.com/user-attachments/assets/6bce8fa5-4655-44c1-a69b-c8d41e624613" />

[https://raw.githubusercontent.com/carolinelile/DE_Zoomcamp_Final_Project/refs/heads/main/citibike_2019_monthly_heatmap_toggle.html]



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

