# Databricks notebook source
#import pyspark functions
from pyspark.sql.functions import*
#import URL processsing
import urllib

# COMMAND ----------

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"
# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# COMMAND ----------

#Get the AWS access keys and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access Key ID').collect()[0]['Access Key ID']
SECRET_KEY = aws_keys_df.select('Secret Access Key').collect()[0]['Secret Access Key']

#Encode the secret key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")


#aws_bucket_name = "user-0afff54d5643-bucket"

# COMMAND ----------

# MAGIC %sql
# MAGIC --Disable format checks during the reading of Delta tables
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

pin_stream_df = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0afff54d5643-pin') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()
display (pin_stream_df)

# COMMAND ----------

geo_stream_df = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0afff54d5643-geo') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()
display (geo_stream_df)

# COMMAND ----------

user_stream_df = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0afff54d5643-user') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()
display (user_stream_df)

# COMMAND ----------

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
geo_schema = StructType([
    StructField("ind", IntegerType()),
    StructField("timestamp", TimestampType()),
    StructField("latitude", FloatType()),
    StructField("longitude", FloatType()),
    StructField("country", StringType())
])
    
df_geo = geo_stream_df.selectExpr("CAST(data as STRING)") \
                .withColumn("data", from_json(col("data"), geo_schema)) \
                .select(col("data.*"))
display(df_geo)


# COMMAND ----------

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

pin_schema = StructType([
    StructField("index", IntegerType()),
    StructField("unique_id", StringType()),
    StructField("title", StringType()),
    StructField("description", StringType()),
    StructField("poster_name", StringType()),
    StructField("follower_count", StringType()),
    StructField("tag_list", StringType()),
    StructField("is_image_or_video", StringType()),
    StructField("image_src", StringType()),
    StructField("downloaded", IntegerType()),
    StructField("save_location", StringType()),
    StructField("category", StringType())
])

df_pin = pin_stream_df.selectExpr("CAST(data as STRING)")\
             .withColumn("data", from_json(col("data"), pin_schema))\
             .select(col("data.*"))

display(df_pin)

# COMMAND ----------


user_schema = StructType([
    StructField("ind", IntegerType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("age", StringType()),
    StructField("date_joined", TimestampType())
])

df_user = user_stream_df.selectExpr("CAST(data as STRING)") \
                .withColumn("data", from_json(col("data"), user_schema)) \
                .select(col("data.*"))
display(df_user)

# COMMAND ----------

# MAGIC %md
# MAGIC Data Cleaning

# COMMAND ----------

dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)

# COMMAND ----------

df_pin.columns


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# Initialize Spark Session
spark = SparkSession.builder.appName("PinDataCleaning").getOrCreate()
# Replace empty and irrelevant entries with None
df_pin = df_pin.na.replace(['', 'NA', 'N/A'], None)
# Convert follower_count to numbers
def convert_follower_count(count):
    if count is None:
        return None
    elif 'k' in count:
        try:
            return int(float(count.replace('k', '')) * 1000)
        except ValueError:
            return None
    elif 'M' in count:
        try:
            return int(float(count.replace('M', '')) * 1000000)
        except ValueError:
            return None
    else:
        try:
            return int(count)
        except ValueError:
            return None

convert_follower_count_udf = udf(convert_follower_count, IntegerType())

# Apply the UDF to the follower_count column
df_pin = df_pin.withColumn('follower_count', convert_follower_count_udf(col('follower_count')))

# Compute the median follower count on a batch DataFrame
df_pin_batch = df_pin.filter(col("follower_count").isNotNull())
df_pin = df_pin.withColumn(
    "follower_count", 
    when(col("follower_count").isNull(), 1000).otherwise(col("follower_count"))
)


# Fill missing values in 'is_image_or_video' and 'poster_name' with 'Unknown'
df_pin = df_pin.withColumn(
    "is_image_or_video", 
    when(col("is_image_or_video").isNull(), "Unknown").otherwise(col("is_image_or_video"))
)
df_pin = df_pin.withColumn(
    "poster_name", 
    when(col("poster_name").isNull(), "Unknown").otherwise(col("poster_name"))
)
df_pin = df_pin.withColumn(
    "category",
    when(col("category").isNull(),
          "Unknown").otherwise(col("category"))
)


# Ensure consistency in 'is_image_or_video'
df_pin = df_pin.withColumn(
    "is_image_or_video",
    when(col("is_image_or_video") == "multi-video(story page format)", "video").otherwise(col("is_image_or_video"))
)

# Removing duplicates
df_pin = df_pin.dropDuplicates()

# Cast 'index' column to IntegerType and rename it to 'ind'
df_pin = df_pin.withColumn('index', col('index').cast(IntegerType()))
df_pin = df_pin.withColumnRenamed('index', 'ind')

# Clean the 'save_location' column
df_pin = df_pin.withColumn('save_location', regexp_replace(col('save_location'), r'Local save in ', ''))

# Reorder the columns as required
df_pin = df_pin.select(
      df_pin['ind'], df_pin['unique_id'], df_pin['title'], df_pin['description'], df_pin['follower_count'], 
    df_pin['poster_name'], df_pin['tag_list'], df_pin['is_image_or_video'], df_pin['image_src'], 
    df_pin['save_location'], df_pin['category']
)

# Show the cleaned DataFrame
df_pin.display()
# Write the streaming DataFrame to a table
df_pin.writeStream \
        .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/kinesis/0afff54d5643_pin_table_checkpoints/") \
            .table("0afff54d5643_pin_table") 
            

    

# COMMAND ----------

df_pin.columns

# COMMAND ----------

# Initialize Spark Session
spark = SparkSession.builder.appName("GeoDataCleaning").getOrCreate()

# Create a new column 'coordinates' that contains an array of 'latitude' and 'longitude'
df_geo = df_geo.withColumn('coordinates', array(col('latitude'), col('longitude')))

# Drop the 'latitude' and 'longitude' columns
df_geo = df_geo.drop('latitude', 'longitude')
df_geo = df_geo.dropna()
# Convert the 'timestamp' column to a timestamp data type
df_geo = df_geo.withColumn('timestamp', col('timestamp').cast(TimestampType()))

# Reorder the DataFrame columns to have the specified order
df_geo = df_geo.select('ind', 'country', 'coordinates', 'timestamp')

# Show the cleaned DataFrame
df_geo.display()
# Write the streaming DataFrame to a table
df_geo.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/kinesis/0afff54d5643_geo_table_checkpoints/") \
    .table("0afff54d5643_geo_table")

# COMMAND ----------

# Initialize Spark Session
spark = SparkSession.builder.appName("UserDataCleaning").getOrCreate()

#Create a new column 'user_name' that concatenates 'first_name' and 'last_name'
df_user = df_user.withColumn('user_name', concat_ws(' ', col('first_name'), col('last_name')))

# Drop the 'first_name' and 'last_name' columns
df_user = df_user.drop('first_name', 'last_name')
df_user.dropna(how='all')

# Convert the 'date_joined' column to a timestamp data type
df_user = df_user.withColumn('date_joined', col('date_joined').cast(TimestampType()))
df_user = df_user.dropna(subset=['ind', 'user_name'])

# Reorder the DataFrame columns to have the specified order
df_user = df_user.select('ind', 'user_name', 'age', 'date_joined')

# Show the cleaned DataFrame
df_user.display()
# Write the streaming DataFrame to a table
df_user.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/kinesis/0afff54d5643_user_table_checkpoints/") \
    .table("0afff54d5643_user_table")
