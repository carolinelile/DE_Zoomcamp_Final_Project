import pyspark
from pyspark.sql import SparkSession, types
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import year as spark_year, month as spark_month
import zipfile
import requests
import os
import glob
from google.cloud import storage
from google.oauth2 import service_account
import folium
from folium.plugins import HeatMap
import pandas as pd
from folium import LayerControl

url_prefix = "https://s3.amazonaws.com/tripdata/"
year_list = ['2019', '2020']

schema_old = types.StructType([
    types.StructField('tripduration', types.LongType(), True), 
    types.StructField('starttime', types.TimestampType(), True), 
    types.StructField('stoptime', types.TimestampType(), True), 
    types.StructField('start station id', types.IntegerType(), True), 
    types.StructField('start station name', types.StringType(), True), 
    types.StructField('start station latitude', types.DoubleType(), True), 
    types.StructField('start station longitude', types.DoubleType(), True), 
    types.StructField('end station id', types.IntegerType(), True), 
    types.StructField('end station name', types.StringType(), True), 
    types.StructField('end station latitude', types.DoubleType(), True), 
    types.StructField('end station longitude', types.DoubleType(), True), 
    types.StructField('bikeid', types.LongType(), True), 
    types.StructField('usertype', types.StringType(), True), 
    types.StructField('birth year', types.IntegerType(), True), 
    types.StructField('gender', types.IntegerType(), True)])
schema_new = types.StructType([
    types.StructField('ride_id', types.StringType(), True), 
    types.StructField('rideable_type', types.StringType(), True), 
    types.StructField('started_at', types.TimestampType(), True), 
    types.StructField('ended_at', types.TimestampType(), True), 
    types.StructField('start_station_name', types.StringType(), True), 
    types.StructField('start_station_id', types.StringType(), True), 
    types.StructField('end_station_name', types.StringType(), True), 
    types.StructField('end_station_id', types.StringType(), True), 
    types.StructField('start_lat', types.DoubleType(), True), 
    types.StructField('start_lng', types.DoubleType(), True), 
    types.StructField('end_lat', types.DoubleType(), True), 
    types.StructField('end_lng', types.DoubleType(), True), 
    types.StructField('member_casual', types.StringType(), True)])


credentials_location = '/home/carolinelile/.google/credentials/service-account.json'
conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('upload_to_gcs') \
    .set("spark.jars", "./lib/gcs-connector-hadoop3-2.2.5.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)
sc = SparkContext(conf=conf)
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()


def download(url, output_path):
    response = requests.get(url)
    with open(output_path, "wb") as f:
        f.write(response.content)
    print(f"1. {output_path} was downloaded.")

def unzip_and_remove(zip_path, year, extract_dir=None):
    if extract_dir is None:
        extract_dir = os.path.splitext(zip_path)[0]    
    os.makedirs(extract_dir, exist_ok=True)
    # Unzip the file
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    # Remove the zip file
    os.remove(zip_path)
    print(f"2. {zip_path} was unzipped and removed.")
    # Recursively check for zip files inside the extracted content
    for root, _, files in os.walk(extract_dir):
        for file in files:
            if file.startswith("._") or "__MACOSX" in root:
                continue
            file_path = os.path.join(root, file)
            if file.endswith(".zip"):                 
                nested_extract_dir = os.path.splitext(file_path)[0]
                try:
                    unzip_and_remove(file_path, year, nested_extract_dir)
                except zipfile.BadZipFile:
                    print(f"*Skipped non-zip file: {file_path}")

def column_rename(df):
    df = df.withColumnRenamed('starttime', 'started_at') \
                           .withColumnRenamed('stoptime', 'ended_at') \
                           .withColumnRenamed('start station name', 'start_station_name') \
                           .withColumnRenamed('end station name', 'end_station_name') \
                           .withColumnRenamed('start station longitude', 'start_lng') \
                           .withColumnRenamed('end station latitude', 'end_lat') \
                           .withColumnRenamed('end station longitude', 'end_lng') \
                           .withColumnRenamed('start station latitude', 'start_lat') \
                           .withColumnRenamed('birth year', 'birth_year')
    return df

def upload_to_gcs(folder, data_year):
    all_csv_files = glob.glob(folder + "/**/*.csv", recursive=True)
    nyc_csvs = [f for f in all_csv_files if not os.path.basename(f).startswith("JC")]
    jc_csvs = glob.glob(folder + "/**/JC*.csv", recursive=True)
    if int(data_year) < 2020:
        schema = schema_old
        nyc_df = spark.read.option("header", "true").schema(schema).csv(nyc_csvs)
        nyc_df = column_rename(nyc_df)
        jc_df = spark.read.option("header", "true").schema(schema).csv(jc_csvs)
        jc_df = column_rename(jc_df)
    else:
        schema_nyc = schema_new
        schema_jc = schema_old
        nyc_df = spark.read.option("header", "true").schema(schema_nyc).csv(nyc_csvs)
        nyc_df = column_rename(nyc_df)
        jc_df = spark.read.option("header", "true").schema(schema_jc).csv(jc_csvs)
        jc_df = column_rename(jc_df)
    df = nyc_df.unionByName(jc_df, allowMissingColumns=True)
    
    df = df.withColumn('year', spark_year(df["started_at"])) \
           .withColumn('month', spark_month(df["started_at"]))
    
    before_count = df.count()
    df = df.filter(
    (df['start_lat'] >= 40.50) & (df['start_lat'] <= 40.92) &
    (df['start_lng'] >= -74.30) & (df['start_lng'] <= -73.68) &
    (df['end_lat'] >= 40.50) & (df['end_lat'] <= 40.92) &
    (df['end_lng'] >= -74.30) & (df['end_lng'] <= -73.68))
    after_count = df.count()
    print(f"* Before count is {before_count}, after filtering the count is {after_count}, difference is {before_count - after_count}.")
    df = df.drop('start station id', 'end station id', 'bikeid', \
                 'ride_id', 'start_station_id', 'end_station_id')

    gcs_path = f"gs://zoomcamp_final_project/citibike/{year}/"
    df.coalesce(1).write.mode("overwrite").parquet(gcs_path)
    # print(df.rdd.getNumPartitions())
    print(f"3. {folder} was uploaded to GCS.")
    return df

def create_monthly_heatmap_layers(df, year):
    # Step 1: Group by month, location and count rides in Spark
    agg_df = (
        df.groupBy("month", "start_lat", "start_lng")
          .count()
          .withColumnRenamed("count", "weight"))
    # Step 2: Convert to Pandas and normalize weights
    pdf = agg_df.toPandas()
    pdf["weight"] = pdf["weight"] / pdf["weight"].max()
    # Step 3: Create base map centered around NYC-JC
    m = folium.Map(location=[40.75, -73.97], zoom_start=11.5)
    # Step 4: Add HeatMap layer per month
    for month in sorted(pdf["month"].unique()):
        month_df = pdf[pdf["month"] == month]
        heat_data = month_df[["start_lat", "start_lng", "weight"]].values.tolist()
        HeatMap(
            heat_data,
            radius=12,
            name=f"{year}-{int(month):02d}",
            show=(month == 1)
        ).add_to(m)
    # Step 5: Add toggle control and save
    folium.LayerControl().add_to(m)
    output_path = f"citibike_{year}_monthly_heatmap_toggle.html"
    m.save(output_path)
    print(f"4. Heatmap: {output_path} was created.")

for year in year_list:
    ### NYC
    ## 1. Download data
    nyc_url = url_prefix + year + '-citibike-tripdata.zip'
    output_path = year + '-citibike-tripdata.zip'
    output_folder = year + '-citibike-tripdata'
    download(nyc_url, output_path)
    ## 2. Unzip and remove
    unzip_and_remove(output_path, year)

    ### JC
    for month in range(1, 13):
        month_str = f"{month:02d}"
        jc_url = f"{url_prefix}JC-{year}{month_str}-citibike-tripdata.csv.zip"
        output_path = f"{year}-citibike-tripdata/JC-{year}{month_str}-citibike-tripdata.zip"
        download(jc_url, output_path)
        unzip_and_remove(output_path, year)

    ## 3. Upload to Data Lake GCS
    df = upload_to_gcs(output_folder, year)

    ## 4. Heat map
    create_monthly_heatmap_layers(df, year)