# Databricks notebook source
#import pyspark functions
from pyspark.sql.functions import*
#import URL processsing
import urllib

# COMMAND ----------

#import pyspark functions
from pyspark.sql.functions import*
#import URL processsing
import urllib

# COMMAND ----------

dbutils.fs.ls("dbfs:/user/hive/warehouse/authentication_credentials")

# COMMAND ----------

#Define the Path to the delta Table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

#Read the Delta table to a spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# COMMAND ----------

#Get the AWS access keys and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access Key ID').collect()[0]['Access Key ID']
SECRET_KEY = aws_keys_df.select('Secret Access Key').collect()[0]['Secret Access Key']

#Encode the secret key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")


#aws_bucket_name = "user-0afff54d5643-bucket"

# COMMAND ----------

#AWS S3 Bucket Name
AWS_S3_BUCKET = "user-0afff54d5643-bucket"
#Mount name for the bucket
MOUNT_NAME = "/mnt/user-0afff54d5643-bucket"
#source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/user-0afff54d5643-bucket/topics"))

# COMMAND ----------

# MAGIC %sql
# MAGIC --Read the JSON format dataset from s3 to databricks
# MAGIC -- Disable format checks during the reading of Delta tables
# MAGIC SET spark.databricks.delta.formatCheck.enabled = false;

# COMMAND ----------

   topics = [".pin",".geo",".user"]
def read_topics_into_df(topics):
    # file location
    # * indicates reading all the content of the specified file that have .json extension
    file_location = f"/mnt/user-0afff54d5643-bucket/topics/0afff54d5643{topics}/partition=0/*.json"
    # file type
    file_type = "json"
    # Ask spark to infer the schema
    infer_schema = "true"
    # Read in JSONs from mounted S3 bucket
    df = spark.read.format(file_type) \
        .option("inferschema", infer_schema) \
        .load(file_location)
    return df

for each_item in topics:
    # create statement strings
    df_statement = f"df_{each_item[1:]} = read_topics_into_df('{each_item}')"
    display_df_statement = f"display(df_{each_item[1:]})"
    exec(df_statement)
    exec(display_df_statement)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# Initialize Spark Session
spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# Read the .pin topic into df_pin
df_pin = read_topics_into_df(".pin")
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

# Fill missing values in 'follower_count' with the median value
median_follower_count = df_pin.approxQuantile("follower_count", [0.5], 0.0)[0]
df_pin = df_pin.withColumn(
    "follower_count", 
    when(col("follower_count").isNull(), median_follower_count).otherwise(col("follower_count"))
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
    'ind', 'unique_id', 'title', 'description', 'follower_count', 
    'poster_name', 'tag_list', 'is_image_or_video', 'image_src', 
    'save_location','category'
)

# Show the cleaned DataFrame
df_pin.display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array
from pyspark.sql.types import TimestampType

# Initialize Spark Session
spark = SparkSession.builder.appName("GeoDataCleaning").getOrCreate()
# Read the .pin topic into df_pin
df_geo = read_topics_into_df(".geo")
# Create a new column 'coordinates' that contains an array of 'latitude' and 'longitude'
df_geo = df_geo.withColumn('coordinates', array(col('latitude'), col('longitude')))

# Drop the 'latitude' and 'longitude' columns
df_geo = df_geo.drop('latitude', 'longitude')

# Convert the 'timestamp' column to a timestamp data type
df_geo = df_geo.withColumn('timestamp', col('timestamp').cast(TimestampType()))

# Reorder the DataFrame columns to have the specified order
df_geo = df_geo.select('ind', 'country', 'coordinates', 'timestamp')

# Show the cleaned DataFrame
df_geo.display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.types import TimestampType
# Initialize Spark Session
spark = SparkSession.builder.appName("UserDataCleaning").getOrCreate()
# Read the .user topic into df_user
df_user = read_topics_into_df(".user")
#Create a new column 'user_name' that concatenates 'first_name' and 'last_name'
df_user = df_user.withColumn('user_name', concat_ws(' ', col('first_name'), col('last_name')))

# Drop the 'first_name' and 'last_name' columns
df_user = df_user.drop('first_name', 'last_name')

# Convert the 'date_joined' column to a timestamp data type
df_user = df_user.withColumn('date_joined', col('date_joined').cast(TimestampType()))

# Reorder the DataFrame columns to have the specified order
df_user = df_user.select('ind', 'user_name', 'age', 'date_joined')

# Show the cleaned DataFrame
df_user.display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, row_number
from pyspark.sql.window import Window
# Initialize Spark Session
spark = SparkSession.builder.appName("PopularCategoryByCountry").getOrCreate()
# Step 1: Join the DataFrames
df_combined = df_pin.join(df_geo, df_pin['ind'] == df_geo['ind'], 'inner') \
                    .join(df_user, df_pin['ind'] == df_user['ind'], 'inner')
# Step 2: Group by country and category, and count the occurrences
df_grouped = df_combined.groupBy("country", "category").agg(count("category").alias("category_count"))

# Step 3: Find the most popular category per country using a window function
window_spec = Window.partitionBy("country").orderBy(col("category_count").desc())
df_most_popular = df_grouped.withColumn("row_number", row_number().over(window_spec)) \
                            .filter(col("row_number") == 1) \
                            .drop("row_number")

# Step 4: Select the relevant columns
df_result = df_most_popular.select("country", "category", "category_count")

# Show the result
df_result.display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, count
# Initialize Spark Session
spark = SparkSession.builder.appName("CategoryPostsByYear").getOrCreate()
# Step 1: Join the DataFrames
df_combined = df_pin.join(df_geo, df_pin['ind'] == df_geo['ind'], 'inner') \
                    .join(df_user, df_pin['ind'] == df_user['ind'], 'inner')
# Step 2: Extract the year from the 'timestamp' column and create 'post_year'
df_combined = df_combined.withColumn('post_year', year(col('timestamp')))
# Step 3: Filter data to include only posts from 2018 to 2022
df_filtered = df_combined.filter((col('post_year') >= 2018) & (col('post_year') <= 2022))
# Step 4: Group by 'post_year' and 'category', and count the occurrences
df_grouped = df_filtered.groupBy('post_year', 'category').agg(count('category')
                                                        .alias('category_count'))
# Step 5: Select the relevant columns
df_result = df_grouped.select('post_year', 'category', 'category_count')
df_result.display()                        

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, row_number
from pyspark.sql.window import Window
# Initialize Spark Session
spark = SparkSession.builder.appName("TopUserByCountry").getOrCreate()
df_combined = df_pin.join(df_geo, df_pin['ind'] == df_geo['ind'], 'inner') \
                    .join(df_user, df_pin['ind'] == df_user['ind'], 'inner')
# Step 2: Group by country and poster_name, summing the follower_count
df_grouped = df_combined.groupBy('country', 'poster_name').agg(sum('follower_count').alias('follower_count'))
# Step 3: Find the user with the most followers per country using a window function
window_spec = Window.partitionBy('country').orderBy(col('follower_count').desc())

df_top_user = df_grouped.withColumn('row_number', row_number().over(window_spec)) \
                        .filter(col('row_number') == 1) \
                        .select('country', 'poster_name', 'follower_count')

# Step 4: show results for the step1 
df_top_user.display()


# COMMAND ----------

#find the country with the user who has the most followers across all countries
window_spec_global = Window.orderBy(col('follower_count').desc())

df_top_country = df_top_user.withColumn('row_number', row_number().over(window_spec_global)) \
                                       .filter(col('row_number') == 1) \
                                       .select('country', 'follower_count')
df_top_country.display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Initialize Spark Session
spark = SparkSession.builder.appName("PopularCategoryByAgeGroup").getOrCreate()

# Step 2: Create age_group based on age
df_combined = df_combined.withColumn('age_group', 
                                     when(col('age').between(18, 24), '18-24')
                                     .when(col('age').between(25, 35), '25-35')
                                     .when(col('age').between(36, 50), '36-50')
                                     .when(col('age') > 50, '+50')
                                     .otherwise('Unknown'))
# Step 3: Group by age_group and category, and count the occurrences
df_grouped = df_combined.groupBy('age_group', 'category').agg(count('category').alias('category_count'))

# Step 4: Find the most popular category for each age group
window_spec = Window.partitionBy('age_group').orderBy(col('category_count').desc())

df_most_popular = df_grouped.withColumn('row_number', row_number().over(window_spec)) \
                            .filter(col('row_number') == 1) \
                            .drop('row_number')

# Show the result
df_result = df_most_popular.select('age_group', 'category', 'category_count')
df_result.display()

# COMMAND ----------

# find the median follower count based on their age group
#Create age groups based on the 'age' column
df_combined = df_combined.withColumn('age_group', 
    when((col('age') >= 18) & (col('age') <= 24), '18-24')
    .when((col('age') >= 25) & (col('age') <= 35), '25-35')
    .when((col('age') >= 36) & (col('age') <= 50), '36-50')
    .when(col('age') > 50, '+50')
    .otherwise('Unknown')
)

# Step 3: Calculate the median follower count for each age group
# To calculate the median, we will use a window function to order the follower_count and then select the middle value
window_spec = Window.partitionBy('age_group').orderBy(col('follower_count'))

# Count the number of rows for each age group
df_combined = df_combined.withColumn("row_number", expr("row_number() over (partition by age_group order by follower_count)"))
df_combined = df_combined.withColumn("total_count", expr("count(*) over (partition by age_group)"))

# Filter to get the median (middle) value
df_median = df_combined.filter((col("row_number") == col("total_count") / 2) | 
                               (col("row_number") == col("total_count") / 2 + 1)) \
                       .groupBy("age_group") \
                       .agg(expr("percentile_approx(follower_count, 0.5)").alias("median_follower_count"))

# Step 4: Select the relevant columns
df_result = df_median.select('age_group', 'median_follower_count')

# Show the result
df_result.display()

# COMMAND ----------

#Find how many users joined in each year
# Step 1: Extract the year from the 'date_joined' column and create 'post_year'
df_user = df_user.withColumn('post_year', year(col('date_joined')))

# Step 2: Filter data to include only users who joined between 2015 and 2020
df_filtered = df_user.filter((col('post_year') >= 2015) & (col('post_year') <= 2020))

# Step 3: Group by 'post_year' and count the number of users who joined
df_grouped = df_filtered.groupBy('post_year').agg(count('*').alias('number_users_joined'))

# Step 4: Select the relevant columns
df_result = df_grouped.select('post_year', 'number_users_joined')

# Show the result
df_result.display()

# COMMAND ----------

#SQL query to create age group column
# import for performing window functions
from pyspark.sql.window import Window
# join df_pin and df_geo dataframes on index
pin_geo = df_pin.join(df_geo, df_pin.ind == df_geo.ind)
# join df_pin and df_user and create temp view for SQL query
df_pin.join(df_user, df_pin.ind == df_user.ind).createOrReplaceTempView("category_age")
pin_user_age_group = spark.sql(
    "SELECT CASE \
        WHEN age between 18 and 24 then '18-24' \
        WHEN age between 25 and 35 then '25-35' \
        WHEN age between 36 and 50 then '36-50' \
        WHEN age > 50 then '50+' \
        END as age_group, * FROM category_age")

# COMMAND ----------

#find the median follower count of users based on their joining year
pin_user_age_group \
.select("user_name", "date_joined", "follower_count") \
.distinct() \
.withColumn("post_year", year("date_joined")) \
.groupBy("post_year") \
.agg(percentile_approx("follower_count", 0.5).alias("median_follower_count")) \
.orderBy("post_year") \
.show()

# COMMAND ----------

# find out how many users joined each year
df_user.withColumn("post_year", year("date_joined")) \
.drop("ind") \
.distinct() \
.groupBy("post_year") \
.agg(count("user_name").alias("number_users_joined")) \
.orderBy("post_year") \
.show()

# COMMAND ----------

# find the median follower count of users based on their joining year
pin_user_age_group \
.select("user_name", "date_joined", "follower_count") \
.distinct() \
.withColumn("post_year", year("date_joined")) \
.groupBy("post_year") \
.agg(percentile_approx("follower_count", 0.5).alias("median_follower_count")) \
.orderBy("post_year") \
.show()

# COMMAND ----------

# find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of
pin_user_age_group \
.select("user_name", "age_group", "date_joined", "follower_count") \
.distinct() \
.withColumn("post_year", year("date_joined")) \
.groupBy("post_year", "age_group") \
.agg(percentile_approx("follower_count", 0.5).alias("median_follower_count")) \
.orderBy("post_year", "age_group") \
.show()

# COMMAND ----------

# unmount the bucket from the filestore
dbutils.fs.unmount("/mnt/user-0afff54d5643-bucket")

# COMMAND ----------


