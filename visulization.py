import pyspark
from pyspark.sql import SparkSession, types
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from google.cloud import storage
from google.oauth2 import service_account
import folium
from folium.plugins import HeatMap, FeatureGroupSubGroup

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

df_2019 = spark.read.parquet('gs://zoomcamp_final_project/citibike/2019/**.parquet')
df_2020 = spark.read.parquet('gs://zoomcamp_final_project/citibike/2020/**.parquet')


pdf_2019 = df_2019.select("start_lat", "start_lng").dropna().toPandas()
pdf_2020 = df_2020.select("start_lat", "start_lng").dropna().toPandas()
heat_data_2019 = pdf_2019[["start_lat", "start_lng"]].values.tolist()
heat_data_2020 = pdf_2020[["start_lat", "start_lng"]].values.tolist()
m = folium.Map(location=[40.75, -73.97], zoom_start=12)
fg = folium.FeatureGroup(name='Base Map').add_to(m)
fg_2019 = folium.FeatureGroup(name='2019-01').add_to(m)
fg_2020 = folium.FeatureGroup(name='2019-05').add_to(m)
HeatMap(heat_data_2019, radius=12).add_to(fg_2019)
HeatMap(heat_data_2020, radius=12).add_to(fg_2020)
folium.LayerControl().add_to(m)
m.save("heatmap_01_vs_05_toggle.html")


credentials = service_account.Credentials.from_service_account_file(
    "/home/carolinelile/.google/credentials/service-account.json")