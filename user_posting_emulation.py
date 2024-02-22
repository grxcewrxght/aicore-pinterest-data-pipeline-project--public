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


def fetch_random_row(connection, table_name, random_row):
    """
    Fetches a random row from the specified table in the database.
    
    Args:
        connection (Connection): SQLAlchemy database connection object.
        table_name (str): Name of the table to fetch data from.
        random_row (int): Index of the random row to fetch.
        
    Returns:
        dict: Dictionary containing the data from the fetched row.
    """
    query = text(f"SELECT * FROM {table_name} LIMIT {random_row}, 1")
    selected_row = connection.execute(query)
    for row in selected_row:
        return dict(row._mapping)


def create_payload(data):
    """
    Creates a JSON payload from the provided data.
    
    Args:
        data (dict): Dictionary containing the data to be included in the payload.
        
    Returns:
        str: JSON representation of the payload.
    """
    payload = {
        "records": [
            {
                "value": data
            }
        ]
    }
    return json.dumps(payload)


def post_data_to_topic(topic, base_url, headers, data):
    """
    Posts data to a specific topic using the provided URL, headers, and payload.
    
    Args:
        topic (str): The topic to post data to.
        base_url (str): The base URL of the API endpoint.
        headers (dict): Headers for the HTTP request.
        data (str): JSON payload containing the data to be posted.
        
    Returns:
        None
    """
    invoke_url = f"{base_url}/topics/12a3da8f7ced.{topic}"
    response = requests.post(invoke_url, headers=headers, data=data)
    
    if response.status_code == 200:
        print(f"{topic.capitalize()} data posted successfully")
    else:
        print(f"Error posting {topic} data. Status Code: {response.status_code}, Content: {response.text}")


def run_infinite_post_data_loop():
    """
    A function to continuously post data to an API endpoint.
    """
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    base_url = "https://amdpzoul4j.execute-api.us-east-1.amazonaws.com/test"
    topics = ["pin", "geo", "user"]

    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = AWSDBConnector().create_db_connector()

        with engine.connect() as connection:
            pin_result = fetch_random_row(connection, "pinterest_data", random_row)
            geo_result = fetch_random_row(connection, "geolocation_data", random_row)
            user_result = fetch_random_row(connection, "user_data", random_row)

            payload_pin = create_payload({
                "index": pin_result['index'], "unique_id": pin_result["unique_id"],
                "title": pin_result["title"], "description": pin_result["description"],
                "poster_name": pin_result["poster_name"], "follower_count": pin_result["follower_count"],
                "tag_list": pin_result["tag_list"], "is_image_or_video": pin_result["is_image_or_video"],
                "image_src": pin_result["image_src"], "downloaded": pin_result["downloaded"],
                "save_location": pin_result["save_location"], "category": pin_result["category"]
            })

            payload_geo = create_payload({
                "index": geo_result["ind"], "country": geo_result["country"],
                "timestamp": geo_result["timestamp"].isoformat(),
                'latitude': geo_result["latitude"], "longitude": geo_result["longitude"]
            })

            payload_user = create_payload({
                "index": user_result["ind"], "first_name": user_result["first_name"],
                "last_name": user_result["last_name"], "age": user_result["age"],
                "date_joined": user_result["date_joined"].isoformat()
            })

            for topic in topics:
                if topic == "pin":
                    topic_data = payload_pin
                elif topic == "geo":
                    topic_data = payload_geo
                elif topic == "user":
                    topic_data = payload_user

                post_data_to_topic(topic, base_url, headers, topic_data)


if __name__ == "__main__":
    run_infinite_post_data_loop()
