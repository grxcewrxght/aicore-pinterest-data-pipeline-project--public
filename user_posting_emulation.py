import requests
from time import sleep
import random
import json
import sqlalchemy
from sqlalchemy import text
import yaml


random.seed(100)


class AWSDBConnector:
    """
    A class for connecting to a database using AWS credentials.

    Attributes:
        HOST (str): The database host address.
        USER (str): The database username.
        PASSWORD (str): The database password.
        DATABASE (str): The name of the database.
        PORT (int): The port number for the database connection.
    """

    def __init__(self, credentials_path='db_creds.yaml'):
        """
        Initializes the AWSDBConnector class with database credentials.

        Args:
            credentials_path (str): Path to the YAML file containing database credentials.
                                    Defaults to 'db_creds.yaml'.
        """
        with open(credentials_path, 'r') as file:
            credentials = yaml.safe_load(file)

        database_info = credentials.get('DATABASE', {})

        self.HOST = database_info.get('HOST', 'default_host')
        self.USER = database_info.get('USER', 'default_user')
        self.PASSWORD = database_info.get('PASSWORD', 'default_password')
        self.DATABASE = database_info.get('NAME', 'default_database')
        self.PORT = database_info.get('PORT', 3306)

    
    def create_db_connector(self):
        """
        Creates a SQLAlchemy database engine using the provided credentials.

        Returns:
            Engine: A SQLAlchemy database engine.
        """
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    """
    A function to continuously post data to an API endpoint.
    """
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

    # Create an infinite loop for continuous data posting
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()
        base_url = "https://amdpzoul4j.execute-api.us-east-1.amazonaws.com/test"
        topics = ["pin", "geo", "user"]

        # Connect to the database 
        with engine.connect() as connection:

            # Fetch random rows from the database
            
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

            
            # Prepare JSON payloads for posting data to the API
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


            # Iterate over topics and post data to the API
            for topic in topics:
                invoke_url = f"{base_url}/topics/12a3da8f7ced.{topic}"

                # Select payload based on the topic 
                if topic == "pin":
                    topic_data = payload_pin
                elif topic == "geo":
                    topic_data = payload_geo
                elif topic == "user":
                    topic_data = payload_user

                # Make a HTTP POST request to the API
                response = requests.request("POST", invoke_url, headers=headers, data=topic_data)

                # Print status message based on the response 
                if response.status_code == 200:
                    print(f"{topic.capitalize()} data posted successfully")
                else:
                    print(f"Error posting {topic} data. Status Code: {response.status_code}, Content: {response.text}")
