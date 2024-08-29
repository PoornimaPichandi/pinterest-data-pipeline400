import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import os
import yaml
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
        print(f"Current state of self.creds: {self.creds}")
        if self.creds:
            engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.creds['RDS_USER']}:{self.creds['RDS_PASSWORD']}@{self.creds['RDS_HOST']}:{self.creds['RDS_PORT']}/{self.creds['RDS_DATABASE']}?charset=utf8mb4")
            print(f"Engine created: {engine}")
            return engine
        else:
            raise ValueError("No Credentials available to create the database connection")

new_connector = AWSDBConnector()

def send_data_to_kafka(invoke_url, payload):
    headers = {'Content-Type':'application/vnd.kafka.json.v2+json'}
    response = requests.post(invoke_url, headers=headers, data=payload)
    if response.status_code == 200:
        print(f"Sent data to {invoke_url} successfully")
    else:
        print(f"Failed to send data to {invoke_url}: {response.status_code} - {response.text}")

def datetime_handler(x):
    if isinstance(x, datetime):
        return x.isoformat()
    raise TypeError("Type not serializable")

def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()
        pin_string_invoke_url = "https://g8dvnn0wp2.execute-api.us-east-1.amazonaws.com/prod/topics/0afff54d5643.pin"
        geo_string_invoke_url = "https://g8dvnn0wp2.execute-api.us-east-1.amazonaws.com/prod/topics/0afff54d5643.geo"
        user_string_invoke_url = "https://g8dvnn0wp2.execute-api.us-east-1.amazonaws.com/prod/topics/0afff54d5643.user"

        with engine.connect() as connection:

            # Sending Pinterest data
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            for row in pin_selected_row:
                pin_result = dict(row._mapping) 
                pin_payload = json.dumps({
                    "records": [
                        {"value": {
                            "index": pin_result["index"],
                            "unique_id": pin_result["unique_id"],
                            "title": pin_result["title"],
                            "description": pin_result["description"],
                            "poster_name": pin_result["poster_name"],
                            "follower_count": pin_result["follower_count"],
                            "tag_list": pin_result["tag_list"],
                            "is_image_or_video": pin_result["is_image_or_video"],
                            "image_src": pin_result["image_src"],
                            "downloaded": pin_result["downloaded"],
                            "save_location": pin_result["save_location"],
                            "category": pin_result["category"]                            
                        }}
                    ]
                }, default=datetime_handler)
                send_data_to_kafka(pin_string_invoke_url, pin_payload)

            # Sending Geolocation data
            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                geo_payload = json.dumps({
                    "records":[
                        {"value":{
                            "ind": geo_result["ind"],
                            "timestamp": geo_result["timestamp"],
                            "latitude": geo_result["latitude"],
                            "longitude": geo_result["longitude"],
                            "country": geo_result["country"]
                        }}
                    ]
                }, default=datetime_handler)
                send_data_to_kafka(geo_string_invoke_url, geo_payload)

            # Sending User data
            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            for row in user_selected_row:
                user_result = dict(row._mapping)
                user_payload = json.dumps({
                    "records": [
                        {"value": {
                            "ind": user_result["ind"],
                            "first_name": user_result["first_name"],
                            "last_name": user_result["last_name"],
                            "age": user_result["age"],
                            "date_joined": user_result["date_joined"]
                        }}
                    ]
                }, default=datetime_handler)
                send_data_to_kafka(user_string_invoke_url, user_payload)
                
            print(f"pin_result: {pin_result}")
            print(f"geo_result: {geo_result}")
            print(f"user_result: {user_result}")

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')