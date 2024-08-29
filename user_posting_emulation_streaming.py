import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
import os
import yaml
from sqlalchemy import text
from datetime import datetime

random.seed(100)


class AWSDBConnector:
    filepath = 'C:/Users/poorn/Documents/Python Scripts/git_repo/pinterest-data-pipeline400/db_creds.yaml'

    def __init__(self, filepath=None):
        if filepath is None:
            filepath = AWSDBConnector.filepath
        
        print(f"Looking for credentials file at: {filepath}")

        if os.path.exists(filepath):
            with open(filepath, 'r') as file:
                self.creds = yaml.load(file, Loader=yaml.FullLoader)
                print(f"Credentials loaded successfully: {self.creds}")
        else:
            print(f'File not found: {filepath}')
            self.creds = None 
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.creds['RDS_USER']}:{self.creds['RDS_PASSWORD']}@{self.creds['RDS_HOST']}:{self.creds['RDS_PORT']}/{self.creds['RDS_DATABASE']}?charset=utf8mb4")
        return engine

new_connector = AWSDBConnector()

def send_data_to_kinesis(stream_url, payload):
    headers = {'Content-Type':'application/json'}
    response = requests.put(stream_url, headers=headers, data=payload)
    if response.status_code == 200:
        print(f"Sent data to {stream_url} successfully")
    else:
        print(f"Failed to send data to {stream_url}: {response.status_code} - {response.text}")

def datetime_handler(x):
    if isinstance(x, datetime):
        return x.isoformat()
    raise TypeError("Type not serializable")

def run_infinite_post_data_loop():
    kinesis_client = boto3.client('kinesis', region_name='us-east-1')
    pin_stream_url = "https://g8dvnn0wp2.execute-api.us-east-1.amazonaws.com/prod/streams/streaming-0afff54d5643-pin/record"
    geo_stream_url = "https://g8dvnn0wp2.execute-api.us-east-1.amazonaws.com/prod/streams/streaming-0afff54d5643-geo/record"
    user_stream_url = "https://g8dvnn0wp2.execute-api.us-east-1.amazonaws.com/prod/streams/streaming-0afff54d5643-user/record"
   

    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            # Sending Pinterest data
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            for row in pin_selected_row:
                pin_stream_result = dict(row._mapping)
                pin_payload = {                    
                    "index": pin_stream_result["index"],
                    "unique_id": pin_stream_result["unique_id"],
                    "title": pin_stream_result["title"],
                    "description": pin_stream_result["description"],
                    "poster_name": pin_stream_result["poster_name"],
                    "follower_count": pin_stream_result["follower_count"],
                    "tag_list": pin_stream_result["tag_list"],
                    "is_image_or_video": pin_stream_result["is_image_or_video"],
                    "image_src": pin_stream_result["image_src"],
                    "downloaded": pin_stream_result["downloaded"],
                    "save_location": pin_stream_result["save_location"],
                    "category": pin_stream_result["category"]
                }
                send_data_to_kinesis(pin_stream_url, pin_payload)

            # Sending Geolocation data
            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            for row in geo_selected_row:
                geo_stream_result = dict(row._mapping)
                geo_payload = {
                    
                    "ind": geo_stream_result["ind"],
                    "timestamp": geo_stream_result["timestamp"],
                    "latitude": geo_stream_result["latitude"],
                    "longitude": geo_stream_result["longitude"],
                    "country": geo_stream_result["country"]
                }
                send_data_to_kinesis(geo_stream_url, geo_payload)

            # Sending User data
            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            for row in user_selected_row:
                user_stream_result = dict(row._mapping)
                user_payload = {
                    
                    "ind": user_stream_result["ind"],
                    "first_name": user_stream_result["first_name"],
                    "last_name": user_stream_result["last_name"],
                    "age": user_stream_result["age"],
                    "date_joined": user_stream_result["date_joined"]
                }
                send_data_to_kinesis(user_stream_url, user_payload)
                
            print(f"pin_stream_result: {pin_stream_result}")
            print(f"geo_stream_result: {geo_stream_result}")
            print(f"user_stream_result: {user_stream_result}")

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    


