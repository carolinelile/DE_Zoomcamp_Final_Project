# Citi Bike Data Pipeline (2019-2020)
This repository contains the final project for the [January–April 2025 DE Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp). It implements a data pipeline to extract, transform, load and visualize Citi Bike trip data for both New York City and Jersey City for the years **2019** and **2020**.

---

## Introduction

Citi Bike is New York City and Jersey City's bike share system and one of the largest and most popular in the United States. Launched in May 2013, Citi Bike quickly became an integral part of the cities’ transportation infrastructure. By 2019 and 2020, the system saw significant expansion, with thousands of new docking stations added across various neighborhoods. These two years marked a period of rapid growth and changing ridership habits, shaped by new ways people chose to get around the city and the sudden impact of the COVID-19 pandemic in 2020.

Analyzing Citi Bike data from this period provides valuable insights into how people moved through the city, how ridership fluctuated across seasons, how station usage evolved, and how different age, gender groups and customer types engaged with the service.

---

## Pipeline Overview
The pipeline covers the following steps:

1. **Data Extraction and Transformation**
2. **Batch processing to load data into Google Cloud Storage using Spark**
3. **Data moving from GCS to BigQuery using Kestra**
4. **Data Visualization including toggleable heatmap layers and interactive dashboard using Metabase**

---

## Pipeline Walk-through

### 1. **Connecting to GCP Virtual Machine**
- The pipeline starts with connecting to a VM instance on GCP, which serves as the execution environment for downloading, unzipping, and processing the raw Citi Bike data.
  
### 2. **Data Extraction**
- **Data Source:** Citi Bike trip history data is downloaded from [citybikenyc.com](https://citibikenyc.com/system-data).
- **Why 2019 and 2020?**  
The years 2019 and 2020 were selected because, starting in January 2020 (and February 2021 for Jersey City), the datasets no longer include key demographic variables such as year of birth and gender. Including these years allows for demographic distribution analysis in the results. In addition, Citi Bike significantly expanded its coverage during this period, adding 85 new stations across Brooklyn and Queens in 2019, and extending into the Bronx and Upper Manhattan with over 100 new stations in 2020, which was driven by increased reliance on bicycles during COVID-19. Visualizing these station expansions can help explain ridership trends.
- **Unpacking and Recursive Unzipping:**  
After downloading the annual NYC zip folders locally, each file is unzipped to extract the monthly trip data. Some of these folders may contain embedded zip files, which also need to be unzipped. A recursive unzipping function is implemented to ensure that all nested zip files are fully extracted for downstream processing. For Jersey City, the data is provided as individual monthly zip files, each of which becomes a CSV file after extraction.

### 3. **Data Transformation and loading to GCS**
- The raw CSV files were read and processed using Spark with appropriate schemas on a yearly basis.
- Records with latitude and longitude values outside the geographic bounds of NYC and Jersey City were filtered out, unnecessary columns were dropped, and additional columns for year and month were added based on the trip start time.
- The Spark DataFrame was then coalesced into a single Parquet file per year and written to a designated path in GCS.

### 4. **Heatmap Visualization**
- Trip counts are aggregated by month and pickup location using Spark to condense large datasets into simplified ride density summaries, reducing memory load and improving visualization performance.
- Ride volumes are then normalized by dividing each count by the maximum monthly ride count, scaling the values between 0 and 1. This step ensures that heatmap intensity reflects relative activity, so high- and low-volume locations are both visually distinguishable. Normalization also ensures that heatmaps generated for different months and years are displayed on a consistent scale. For example, even though the heatmaps for March 2019 and March 2020 are rendered on separate maps, they use the same intensity scale, enabling fair and reliable comparisons.
- A Folium map centered roughly at Midtown Manhattan is created, a toggleable heatmap layer is added for each month, and the results are saved yearly as an interactive .html file with layer controls.

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

