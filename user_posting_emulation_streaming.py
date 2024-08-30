import requests
from time import sleep
import random
import json
import sqlalchemy
import os
import yaml
from sqlalchemy import text
from datetime import datetime

random.seed(100)

class AWSDBConnector:
    """
    A class used to connect to an AWS RDS MySQL database using credentials from a YAML file.

    Attributes:
        filepath (str): Default file path for the credentials YAML file.
        creds (dict): Dictionary to store credentials after loading from the YAML file.

    Methods:
        __init__(filepath=None): Initializes the AWSDBConnector with the given or default file path.
        create_db_connector(): Creates and returns a SQLAlchemy engine using the credentials.
    """
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

def send_data_to_kinesis(stream_url, payload, max_retries=3):
    """
    Sends data to an AWS Kinesis stream with retry logic.

    Parameters:
        stream_url (str): The URL of the Kinesis stream.
        payload (dict): The data payload to be sent.
        max_retries (int): The maximum number of retries in case of failure. Defaults to 3.
    """
    headers = {'Content-Type': 'application/json'}
    for attempt in range(max_retries):
        try:
            response = requests.put(stream_url, headers=headers, data=json.dumps(payload, default=str))
            if response.status_code == 200:
                print(f"Sent data to {stream_url} successfully")
                print(response.json())
                return
            else:
                print(f"Failed to send data to {stream_url}: {response.status_code} - {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
        
        # Wait before retrying
        sleep(2 ** attempt)
    
    print(f"Failed to send data to {stream_url} after {max_retries} attempts")

def datetime_handler(x):
        """
    Custom JSON serializer for datetime objects.

    Parameters:
        x (any): The object to serialize.

    Returns:
        str: ISO format string if x is a datetime object.

    Raises:
        TypeError: If the object is not serializable.
    """
        if isinstance(x, datetime):
            return x.isoformat()
        raise TypeError("Type not serializable")

def run_infinite_post_data_loop():
    """
    Continuously fetches random rows from the database and sends them to AWS Kinesis streams.
    
    This function runs an infinite loop where it fetches a random row from Pinterest, 
    geolocation, and user data tables, and then sends each row's data to the corresponding 
    Kinesis stream.
    """
    pin_stream_url = "https://g8dvnn0wp2.execute-api.us-east-1.amazonaws.com/prod/streams/streaming-0afff54d5643-pin/record"
    geo_stream_url = "https://g8dvnn0wp2.execute-api.us-east-1.amazonaws.com/prod/streams/streaming-0afff54d5643-geo/record"
    user_stream_url = "https://g8dvnn0wp2.execute-api.us-east-1.amazonaws.com/prod/streams/streaming-0afff54d5643-user/record"
   
    while True:
        # Sleep for a random interval between 0 and 2 seconds
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000) # Random row number to fetch from the database
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            # Sending Pinterest data
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            for row in pin_selected_row:
                pin_stream_result = dict(row._mapping)
                pin_payload = {    
                    "StreamName": "streaming-0afff54d5643-pin",
                    "Data": {              
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
                },
                "PartitionKey": "1"
                }
                send_data_to_kinesis(pin_stream_url, pin_payload)

            # Sending Geolocation data
            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            for row in geo_selected_row:
                geo_stream_result = dict(row._mapping)
                geo_payload = {
                    "StreamName": "streaming-0afff54d5643-geo",
                    "Data":{
                    "ind": geo_stream_result["ind"],
                    "timestamp": geo_stream_result["timestamp"],
                    "latitude": geo_stream_result["latitude"],
                    "longitude": geo_stream_result["longitude"],
                    "country": geo_stream_result["country"]
                },
                "PartitionKey": "1"
                }
                send_data_to_kinesis(geo_stream_url, geo_payload)

            # Sending User data
            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            for row in user_selected_row:
                user_stream_result = dict(row._mapping)
                user_payload = {
                    "StreamName": "streaming-0afff54d5643-user",
                    "Data":{
                    "ind": user_stream_result["ind"],
                    "first_name": user_stream_result["first_name"],
                    "last_name": user_stream_result["last_name"],
                    "age": user_stream_result["age"],
                    "date_joined": user_stream_result["date_joined"]
                },
                "PartitionKey": "1"
                }
                send_data_to_kinesis(user_stream_url, user_payload)
                
            print(f"pin_stream_result: {pin_stream_result}")
            print(f"geo_stream_result: {geo_stream_result}")
            print(f"user_stream_result: {user_stream_result}")

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')