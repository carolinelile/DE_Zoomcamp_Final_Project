import pyspark
from pyspark.sql import SparkSession, types
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import year as spark_year, month as spark_month
import zipfile
import requests
import os
import glob


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
            #     else:
            #         os.remove(file_path)
            #         print(f"2. {file_path} was removed.")
            # elif (year + month) not in file:
            #     os.remove(file_path)
            #     print(f"2. {file_path} was removed.")
   
def upload_to_gcs(folder, data_year):
    all_csv_files = glob.glob(folder + "/**/*.csv", recursive=True)
    if int(data_year) < 2020:
        schema = schema_old
        df = spark.read.option("header", "true").schema(schema).csv(all_csv_files)
        df = df.withColumnRenamed('starttime', 'started_at') \
               .withColumnRenamed('stoptime', 'ended_at') \
               .withColumnRenamed('start station name', 'start_station_name') \
               .withColumnRenamed('end station name', 'end_station_name') \
               .withColumnRenamed('start station longitude', 'start_lng') \
               .withColumnRenamed('end station latitude', 'end_lat') \
               .withColumnRenamed('end station longitude', 'end_lng') \
               .withColumnRenamed('start station latitude', 'start_lat') \
               .withColumnRenamed('birth year', 'birth_year')
    else:
        schema = schema_new
        df = spark.read.option("header", "true").schema(schema).csv(all_csv_files)
    
    df = df.withColumn('year', spark_year(df["started_at"])) \
           .withColumn('month', spark_month(df["started_at"]))
    
    before_count = df.count()
    df = df.filter(
    (df['start_lat'] >= 40.4774) & (df['start_lat'] <= 40.9176) &
    (df['start_lng'] >= -74.2591) & (df['start_lng'] <= -73.7004) &
    (df['end_lat'] >= 40.4774) & (df['end_lat'] <= 40.9176) &
    (df['end_lng'] >= -74.2591) & (df['end_lng'] <= -73.7004))
    after_count = df.count()
    print(f"Before count is {before_count}, after counter is {after_count}, difference is {before_count - after_count}.")
    df = df.drop('start station id', 'end station id', 'bikeid', \
                 'ride_id', 'start_station_id', 'end_station_id')

    gcs_path = f"gs://zoomcamp_final_project/citibike/{year}/"
    df.coalesce(1).write.mode("overwrite").parquet(gcs_path)
    # print(df.rdd.getNumPartitions())
    print(f"3. {folder} was uploaded to GCS.")
    # df.show()
    # spark.conf.set('temporaryGcsBucket', 'zoomcamp_temp_bucket')
    # df.write.format('bigquery') \
    # .option('table', 'stripe.test') \
    # .option('temporaryGcsBucket', 'zoomcamp_temp_bucket') \
    # .mode('overwrite') \
    # .save()
    # Centered around NYC


################## heat maps
# pandas_df = df.select("start_lat", "start_lng").dropna().toPandas()
# m = folium.Map(location=[40.75, -73.97], zoom_start=12)
# # Prepare the data
# heat_data = pandas_df[["start_lat", "start_lng"]].values.tolist()
# # Add HeatMap layer
# HeatMap(heat_data, radius=12).add_to(m)
# # Save as HTML (you can open this in a browser)
# m.save(f"{data_year}_heatmap_example.html")

  





# def create_bigquery_table(year):
#     # spark = SparkSession.builder \
#     # .appName("BQWriter") \
#     # .config("spark.jars", "./lib/spark-bigquery-latest_2.12.jar") \
#     # .getOrCreate()

#     # df = spark.read.parquet(f"gs://zoomcamp_final_project/citibike/{year}/")

#     # df.write.format("bigquery") \
#     #     .option("table", f"zoomcamp_final_project.citibike/{year}/") \
#     #     .option("temporaryGcsBucket", "zoomcamp_temp_bucket") \
#     #     .mode("overwrite") \
#     #     .save()

#     client = bigquery.Client()
#     project_id = "de-zoomcamp-449923"
#     dataset_id = "stripe"
#     table_id = f"{project_id}.{dataset_id}.citibike"
#     gcs_path = f"gs://zoomcamp_final_project/citibike/{year}/**.parquet"
#     # try:
#     client.get_dataset(f"{project_id}.{dataset_id}")
#     # except Exception:
#     #     dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
#     #     dataset.location = "US"
#     #     client.create_dataset(dataset, exists_ok=True)
#     job_config = bigquery.LoadJobConfig(
#         source_format=bigquery.SourceFormat.PARQUET,
#         autodetect=True,
#         write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # overwrites if exists
#     )
#     load_job = client.load_table_from_uri(
#         gcs_path, table_id, job_config=job_config
#     )
#     load_job.result() 
#     print("Loaded table:", table_id)


for year in year_list:
    ## 1. Download data
    nyc_url = url_prefix + year + '-citibike-tripdata.zip'
    output_path = year + '-citibike-tripdata.zip'
    output_folder = year + '-citibike-tripdata'
    download(nyc_url, output_path)

    ## 2. Unzip and remove
    unzip_and_remove(output_path, year)

    ## 3. Upload to Data Lake GCS
    upload_to_gcs(output_folder, year)

