# Pinterest Data Pipeline
   -  pinterest-data-pipeline400


# Table of Contents:
-  Project Aim
-  Software Requirements
-  Prerequisites
-  Usage Instructions
   -  Set Up the environment
   -  Build the infrastructure
   -  Batch Processing configure the EC2 kafka 
   -  Batch Processsing : Connect a MSK cluster to a s3 bucket
   -  Configuring an API in API Gateway
   -  Batch Processing: Databricks
   -  Batch Processing: Spark on Databricks
   -  Batch Processing: AWS MWAA
   -  Stream Processing : AWS kinesis
-  File Structure of this Project 


# Project Aim:
    In this project, I am developing an infrastructure 
    
    similar to Pinterest, which integrates both historical 
    
    and real-time data, such as user posts.
    
    The infrastructure is built in the AWS cloud and 
    
    processes data events through two distinct pipelines: 
    
    one for real-time data and the other for historical 
    
    data.
   
# Software Requirements:
   - VSCode
   - Postgres SQL
   - WSL
   - AWS
        1. AWS EC2
        2. Postgresql
        3. Amazon S3 
        4. Amazon MSK
        5. AWS API Gateway
        6. AWS Kinesis
        7. Amazon MWAA
        8. CloudWatch
        9. IAM
        10. DataBricks  
           
 

# Prerequisites:
   - To achieve th aim of this project learned utilized 
   - Required Packages in Python
   
   1. pip install pyYAML  
   2. pip install tabula-py
   3. pip installsqlalchemy
   4. pip install pandas
   5. pip install requests
   6. pip install boto3
   7. pip install sqlAlchemy
   8. pip install kafka
   9. pip install apache-airflow

# Usage Instructions:
  ## Set Up the environment:
   - Set up on Github
   - Set up AWS
 ## Build the infrastructure:
  - Download the pinterest infrastructure
  - singin to AWS console
 ## Batch Processing configure the EC2 kafka 
   - Connect to EC2 instance
   - Setup Kafka on the EC2 instance
   - Create kafka Topics
   
 ## Batch Processsing : Connect a MSK cluster to a s3 bucket
   - Create a custom plugin with MSK Connect
   - Create a connector with MSK connect
 ## Configuring an API in API Gateway
   - Build a kafka REST proxy on the EC2 client
   - Send data to the API
 ## Batch Processing: Databricks
   - Setup the Databricks account
   - Mount a S3 bucket to Databricks   
 ## Batch Processing: Spark on Databricks
   - Clean the dataframe contains information about Pinterest Posts, Users, geolocation.
   - Query using pyspark to find the users with most followers in each country
   - Query using pyspark to find the most popular categoryfor each year
   - Query using pyspark to find the most popular category for different age groups
   - Query using pyspark to find the median follower count for different age groups
   - Query using pyspark to find the median follower count of users based on their joining year
   - Query using pyspark to find the median follower count of users based on their joining year and age group
 ## Batch Processing: AWS MWAA
 - Create and upload a DAG to a MWAA environment
 - Trigger a DAG that runs a Databricks Notebook
 ## Stream Processing : AWS kinesis
 - Create data streams using Kinesis Data Streams
 - Configure an API with kinesis proxy integration
 - Send data to the Kinesis streams
 - Read data from Kinesis streams in Databricks
 - Write the streaming data to Delta tables

# File Structure of the Project: 
    --pinterest-data-pipeline400
        |.gitignore
        |.pem
        |(Clone) kinesis_streaming_process_in_databricks.py
        |(Clone) mount_s3_in_databricks.py
        |db_cred.yaml
        |key pair name.pem
        |README.md
        |user_posting_emulation_streaming.py           
        |user_posting_emulation.py       
        |db_creds.yaml.py        
        

