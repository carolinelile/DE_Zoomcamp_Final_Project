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
- The raw CSV files are read and processed using Spark with appropriate schemas on a yearly basis.
- Records with latitude and longitude values outside the geographic bounds of NYC and Jersey City were filtered out, unnecessary columns were dropped, and additional columns for year and month were added based on the trip start time.
- The Spark DataFrame was then coalesced into a single Parquet file per year and written to a designated path in GCS.

### 4. **Heatmap Visualization**
- Trip counts are aggregated by month and pickup location using Spark to condense large datasets into simplified ride density summaries, reducing memory load and improving visualization performance.
- Ride volumes are normalized by dividing each location’s count by the maximum ride count within the same month, scaling values between 0 and 1. This allows the heatmap to reflect relative activity within each month, making it easier to compare location density and highlight both high- and low-volume areas.
- A Folium map centered roughly at Midtown Manhattan is created, a toggleable heatmap layer is added for each month, and the results are saved yearly as an interactive .html file with layer controls.

### 5. **Loading Data from GCS to BigQuery**
- The Parquet file for each year is manually renamed to `{year}.parquet` in the GCS bucket.
- Kestra is used to orchestrate the loading process, which involves the following steps:
  1. **Create an empty final table** in BigQuery with the full schema and all required fields.
  2. **Create an external table** referencing the Parquet file stored in GCS.
  3. **Create a temporary table** from the external table, with a unique row ID generated using an `MD5` hash of three key columns.
  4. **Merge the temporary table** into the final BigQuery table to perform a deduplicated and structured load.

### 6. **Visualization: Dashboard**
A final interactive dashboard was created to explore Citi Bike trends across 2019 and 2020, offering insights into ridership volume, demographics, and trip patterns:
- **40.8 million total trips** were recorded across both years.
- **Trip volume peaked** during summer and early fall, with July–September seeing over 2.4 million rides per month in both years.
- **A sharp drop occurred in April 2020** (692k rides), reflecting the early impact of COVID-19 lockdowns.
- **Pershing Square North** was the most popular starting station with over **231,000 trips**.
- In 2019, the majority of users were **annual members (86.1%)**, with a small portion using short-term passes.
- In 2020, **casual riders made up 23%** of the user base, indicating a slight shift toward non-member usage.
- The **gender distribution in 2019** skewed heavily male (68.4%), with females making up 24%.
- The **age distribution** showed high ridership among users in their 30s and a spike at age 50, possibly due to default birth year values.
The dashboard provides an accessible overview of how Citi Bike usage evolved, helping to contextualize the data pipeline outputs and support deeper exploration.

[metabase_dashboard_link](https://alpakaka.metabaseapp.com/public/dashboard/5e1c5d0f-9d73-48df-aeb7-fa03af231008)

![IMG_0309](https://github.com/user-attachments/assets/c2a2a73a-4c4e-4797-a4f4-6f0ec2f943ef)


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

