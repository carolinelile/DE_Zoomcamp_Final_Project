import pyspark
from pyspark.sql import SparkSession

def download():
    pass

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read.option("header", "true").csv("202401-citibike-tripdata.csv")
df.printSchema()

def unzip():
    pass

def upload_to_gcs():
    pass

def delete_local_data():
    pass
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