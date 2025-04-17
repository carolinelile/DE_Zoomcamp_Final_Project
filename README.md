# Citi Bike Data Pipeline (2019-2020)
This repository contains the final project for the [January–April 2025 DE Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp). It implements a data pipeline to extract, transform, load and visualize Citi Bike trip data for both New York City and Jersey City for the years **2019** and **2020**.

---

## 🚲 Introduction

Citi Bike is New York City and Jersey City's bike share system and one of the largest and most popular in the United States. Launched in May 2013, Citi Bike quickly became an integral part of the cities’ transportation infrastructure. By 2019 and 2020, the system saw significant expansion, with thousands of new docking stations added across various neighborhoods. These two years marked a period of rapid growth and changing ridership habits, shaped by new ways people chose to get around the city and the sudden impact of the COVID-19 pandemic in 2020.

Analyzing Citi Bike data from this period provides valuable insights into how people moved through the city, how ridership fluctuated across seasons, how station usage evolved, and how different gender, age groups and customer types engaged with the service.

---

## ⛓️ Pipeline Overview
The pipeline covers the following steps:

1. **Data Extraction and Transformation**
2. **Batch processing to load data into Google Cloud Storage using Spark**
3. **Data moving from GCS to BigQuery using Kestra**
4. **Data Visualization including toggleable heatmap layers and dashboard using Metabase**

---

## 👣 Pipeline Walk-through

### 1. **Connecting to GCP Virtual Machine**
- The pipeline starts with connecting to a VM instance on GCP, which serves as the execution environment for downloading, unzipping, and processing the raw Citi Bike data.
  
### 2. **Data Extraction**
- **Data Source:** Citi Bike trip history data is downloaded from [citybikenyc.com](https://citibikenyc.com/system-data).
- **Why 2019 and 2020?**  
The years 2019 and 2020 were selected because, starting in January 2020 (and February 2021 for Jersey City), the datasets no longer include key demographic variables such as year of birth and gender. Including these years allows for demographic distribution analysis in the results. In addition, Citi Bike significantly expanded its coverage during this period, adding 85 new stations across Brooklyn and Queens in 2019, and extending into the Bronx and Upper Manhattan with over 100 new stations in 2020, which was driven by increased reliance on bicycles during COVID-19. Visualizing these station expansions can help explain ridership trends.
- **Unpacking and Recursive Unzipping:**  
After downloading the annual NYC zip folders locally, each file is unzipped to extract the monthly trip data. Some of these folders may contain embedded zip files, which also need to be unzipped. A recursive unzipping function is implemented to ensure that all nested zip files are fully extracted for downstream processing. For Jersey City, the data is provided as individual monthly zip files, each of which becomes a CSV file after extraction.

### 3. **Data Transformation and loading into GCS**
- The raw CSV files are read and processed using Spark with appropriate schemas on a yearly basis.
- Records with latitude and longitude values outside the geographic bounds of NYC and Jersey City were filtered out, unnecessary columns were dropped, and additional columns for year and month were added based on the trip start time.
- The Spark DataFrame was then coalesced into a single Parquet file per year and written to a designated path in GCS.

### 4. **Heatmap Visualization**
- Trip counts are aggregated by month and pickup location using Spark to condense large datasets into simplified ride density summaries, reducing memory load and improving visualization performance.
- Ride volumes are normalized by dividing each location’s count by the maximum ride count within the same month, scaling values between 0 and 1. This allows the heatmap to reflect relative activity within each month, making it easier to compare location density and highlight both high- and low-volume areas.
- A Folium map centered roughly at Midtown Manhattan is created, a toggleable heatmap layer is added for each month, and the results are saved yearly as an interactive `.html` file with layer controls.

Here are the links to the toggleable heatmap layers. You can select different months using the menu in the top-right corner. To download a heatmap, right-click the link and choose “Save Link As…”. [2019](https://raw.githubusercontent.com/carolinelile/DE_Zoomcamp_Final_Project/refs/heads/main/citibike_2019_monthly_heatmap_toggle.html)
[2020](https://raw.githubusercontent.com/carolinelile/DE_Zoomcamp_Final_Project/refs/heads/main/citibike_2020_monthly_heatmap_toggle.html)

Heatmap comparison 2019-01 VS 2020-12:

![IMG_0314](https://github.com/user-attachments/assets/cd116911-b2cd-44c6-8386-a8ddb491a3c4)


### 5. **Loading Data from GCS to BigQuery**
- The Parquet file for each year is manually renamed to `{year}.parquet` in the GCS bucket.
- Kestra is used to orchestrate the loading process, which involves the following steps:
  1. **Create an empty final table** in BigQuery with the full schema and all required fields, partitioned by date based on the trip start time.
  2. **Create an external table** in BigQuery referencing the Parquet file stored in GCS.
  3. **Create a temporary table** in BigQuery from the external table, with a unique row ID generated using an `MD5` hash of three key columns.
  4. **Merge the temporary table** into the final BigQuery table to perform a deduplicated and structured load.

### 6. **Dashboard Visualization**
A final interactive dashboard was created using Metabase to explore Citi Bike trends across 2019 and 2020:
- **A total of 40.8 million trips** were recorded across both years, with an **average trip duration of 16.2 minutes** in 2019.
- **Trip volume peaked** during summer and early fall, with July-September seeing over 2 million rides per month in both years.
- **A sharp drop occurred in April 2020** (692k rides), reflecting the early impact of COVID-19 lockdowns.
- **Pershing Square North** (42nd St & Park Ave outside Grand Central) was the most popular starting station with over **231,000 trips**.
- In 2019, the majority of users were **annual members (86.1%)**, with a small portion using short-term passes. Starting in 2020, Citi Bike updated its user type categories to casual and member. That year, **member riders made up 77%** of the user base.
- The **gender distribution in 2019** skewed heavily male, with male riders accounting for 68.4% of users—approximately 2.85 times the number of female riders (24%). An additional 7.6% of users did not specify their gender.
- The **age distribution** showed high ridership among users in their 30s and a spike at age 50, possibly due to default birth year values.

Here's the link for the [Interactive Metabase Dashboard](https://alpakaka.metabaseapp.com/public/dashboard/5e1c5d0f-9d73-48df-aeb7-fa03af231008), probably valid until April 29, 2025.

![IMG_0322](https://github.com/user-attachments/assets/d715ca9f-8b16-4231-9a9d-c99a309bb620)

---

## 🔄 How to Run
1. Install required python libraries:
   ```bash
   pip install pyspark pandas google-cloud-storage folium 
   ```
2. Ensure valid Google Cloud credentials are available at:
   ```
   /home/<user>/.google/credentials/service-account.json
   ```
3. Run the first-step Python script:
   ```bash
   python citibike_et.py
   ```
4. Start a local Kestra session using Docker:
   ```bash
   docker run --pull=always --rm -it -p 8080:8080 --user=root -v /var/run/docker.sock:/var/run/docker.sock -v /tmp:/tmp kestra/kestra:latest server local
   ```
5. Copy the YAML file for the second step into the Kestra flow and execute it.

---

## 🐞 Challenges and Current Limitations

Throughout this project, several technical challenges came up, and certain design trade-offs were made. While the pipeline demonstrates a complete ETL process for handling and visualizing Citi Bike data, the following issues and limitations remain:

### 1. **Manual Download of Source Data**
The source data is currently downloaded from public S3 URLs, extracted locally using Python, and then written to GCS. It relies on local storage and manual orchestration, which is not scalable or cloud-native. Ideally, data ingestion should occur entirely in the cloud — using scheduled jobs to fetch, extract, and write directly to cloud storage without local dependencies. Alternatively, for more flexible and production-ready pipelines, selecting data sources with structured APIs can offer more dynamic, queryable access.

### 2. **Folder Upload Limitation in Kestra**  
Kestra wasn’t able to upload an entire folder to GCS or use flexible file selection methods such as wildcards (`*`) or Python’s `glob` function. If multiple CSVs could have been uploaded directly, the full ELT process — including transformation with dbt — could have been handled entirely within Kestra. This limitation also led to coalescing the Spark output into one Parquet file/year in GCS to simplify the upload process into BigQuery using Kestra, which significantly reduced efficiency. 

### 3. **Spark-to-BigQuery Connector Issue**  
Attempts were made to load data directly from Spark into BigQuery using the Spark–BigQuery connector, but the connection was unsuccessful. Data had to be written to GCS first using Spark, and then loaded into BigQuery using Kestra. This made the ETL process less efficient than a direct Spark-to-BigQuery integration.

### 4. **Limited Orchestration and Containerization**  
The pipeline is not fully orchestrated from end to end. Ideally, a single trigger should initiate the entire workflow without manual steps, with each task depending on the successful completion of the previous one. While Kestra was used to orchestrate part of the flow, the initial Spark script could have been containerized and built into a Docker image and executed within the same `docker-compose` environment as Kestra. In addition, the Parquet file written to GCS by Spark had to be manually renamed before it could be loaded into BigQuery via Kestra, since Spark does not support directly specifying output file names

### 5. Heatmap Visualization Based on Intermediate Data
The heatmaps were generated using data read into Spark and written to GCS, as the connection between Spark and BigQuery was unsuccessful. While this avoids reloading the data, it bypasses the final cleaned dataset in BigQuery, which ideally should be the source of truth for reporting and visualizations. 

### 6. Single Python Script
All tasks in the first step — including downloading data, unzipping files, processing with Spark, and uploading to GCS — were implemented in a single Python script. This made the code harder to test, maintain, and reuse. The script should be refactored into smaller, modular components (such as main and functions scripts) to make the pipeline easier to manage and integrate into an orchestrated workflow.

---

## 👩‍🔧 Future Improvements

To move this project closer to a production-grade pipeline, several structural enhancements could be made to improve orchestration and reproducibility.

### 1. Orchestration  
In a production setup, the pipeline can be orchestrated end-to-end using Airflow, with each of the following steps defined as separate tasks within a DAG:
- **Transfer Raw Data**  
Use `gsutil cp` or a scheduled transfer job to copy monthly Citi Bike .zip files directly from the public AWS S3 bucket to a staging bucket in GCS.
- **Unzip Files in GCS**  
Run a Python-based Cloud Function to unzip the files in memory and write the extracted CSVs back to a specified folder in GCS.
- **Convert to Parquet**  
Use Spark or a lightweight **Python task** to convert the extracted CSVs into Parquet format for optimized storage and faster querying.
- **Load into BigQuery**  
Use `GCSToBigQueryOperator` or a SQL task to load the raw or converted files into a BigQuery staging table.
- **Transform with dbt**  
Trigger `dbt run` via a `BashOperator` to transform the staging data into final, cleaned tables.


### 2. Improving Reproducibility  
To make the pipeline easy for others to run and extend, the following best practices can be adopted:

- **Containerize each component** (e.g. Python scripts, Spark jobs, dbt models) into modular Docker images.
- **Publish Docker images** to Docker Hub or GitHub Container Registry for easy reuse.
- **Store orchestration code** (Airflow DAGs), helper scripts, and configs in a public GitHub repository.
- **Provide a clear `README.md`** with step-by-step instructions, including:
  - How to start Airflow using `docker-compose`
  - How to trigger and monitor the DAG
- **Include a `docker-compose.yml` file** that defines the full orchestrated environment. It should:
  - Use the official Airflow image (e.g. `apache/airflow:2.7.3`)
  - Set up core Airflow services: `webserver`, `scheduler`, and `Postgres` metadata DB
  - Define your custom containers (e.g. Spark-based processing, dbt image)
  - Mount shared volumes (e.g. `/dags`, `/plugins`, `/logs`)
  - Set up environment variables and connection configs (via `.env` or inline)
- With this setup, anyone with Docker installed can clone the repository, run `docker-compose up`, and execute the pipeline through Airflow — ensuring reproducibility and portability across environments.
