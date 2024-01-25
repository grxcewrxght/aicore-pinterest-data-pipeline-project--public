import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import yaml


random.seed(100)


class AWSDBConnector:

    def __init__(self, credentials_path='db_creds.yaml'):
        with open(credentials_path, 'r') as file:
            credentials = yaml.safe_load(file)

        database_info = credentials.get('DATABASE', {})

        self.HOST = database_info.get('HOST', 'default_host')
        self.USER = database_info.get('USER', 'default_user')
        self.PASSWORD = database_info.get('PASSWORD', 'default_password')
        self.DATABASE = database_info.get('NAME', 'default_database')
        self.PORT = database_info.get('PORT', 3306)

    
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()
        base_url = "https://amdpzoul4j.execute-api.us-east-1.amazonaws.com/test"
        topics = ["pin", "geo", "user"]

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)

            for row in user_selected_row:
                user_result = dict(row._mapping)

            
            payload_pin = json.dumps({
                "records": [
                    {
                        "value": {"index": pin_result['index'], "unique_id": pin_result["unique_id"],
                                  "title": pin_result["title"], "description": pin_result["description"],
                                  "poster_name": pin_result["poster_name"], "follower_count": pin_result["follower_count"],
                                  "tag_list": pin_result["tag_list"], "is_image_or_video": pin_result["is_image_or_video"],
                                  "image_src": pin_result["image_src"], "downloaded": pin_result["downloaded"],
                                  "save_location": pin_result["save_location"], "category": pin_result["category"]}
                    }
                ]
            })

            payload_geo = json.dumps({
                "records": [
                    {
                        "value": {"index": geo_result["ind"], "country": geo_result["country"],
                                  "timestamp": geo_result["timestamp"].isoformat(),
                                  'latitude': geo_result["latitude"], "longitude": geo_result["longitude"]}
                    }
                ]
            })

            payload_user = json.dumps({
                "records": [
                    {
                        "value": {"index": user_result["ind"], "first_name": user_result["first_name"],
                                  "last_name": user_result["last_name"], "age": user_result["age"],
                                  "date_joined": user_result["date_joined"].isoformat()}
                    }
                ]
            })

            
            for topic in topics:
                invoke_url = f"{base_url}/topics/12a3da8f7ced.{topic}"

                if topic == "pin":
                    topic_data = payload_pin
                elif topic == "geo":
                    topic_data = payload_geo
                elif topic == "user":
                    topic_data = payload_user

                response = requests.request("POST", invoke_url, headers=headers, data=topic_data)

                if response.status_code == 200:
                    print(f"{topic.capitalize()} data posted successfully")
                else:
                    print(f"Error posting {topic} data. Status Code: {response.status_code}, Content: {response.text}")
