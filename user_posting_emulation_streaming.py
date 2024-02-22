from user_posting_emulation import *

# Base URL for the API endpoint
base_url = "https://amdpzoul4j.execute-api.us-east-1.amazonaws.com/test"

stream_name = "streaming-12a3da8f7ced"
headers = {'Content-Type': 'application/json'}
topics = ["pin", "geo", "user"]


def create_invoke_url(base_url, stream_name, topic):
    """
    Creates the invoke URL for posting data to a specific topic in the Kinesis stream.

    Args:
        base_url (str): The base URL of the API endpoint.
        stream_name (str): The name of the Kinesis stream.
        topic (str): The topic name.

    Returns:
        str: The complete URL for posting data to the specified topic in the Kinesis stream.
    """
    return f"{base_url}/streams/{stream_name}-{topic}/record"


def api_send_to_kinesis(url, headers, table_dict, partition_key, stream_name):
    """
    Posts data to a Kinesis stream.

    Args:
        url (str): The URL for posting data to the Kinesis stream.
        headers (dict): Headers for the HTTP request.
        table_dict (dict): The data payload to be sent to the Kinesis stream.
        partition_key (str): The partition key for the data.
        stream_name (str): The name of the Kinesis stream.

    Returns:
        None
    """
    try:
        payload = {
            "StreamName": stream_name,
            "Data": table_dict,
            "PartitionKey": partition_key
        }

        response = requests.post(url, headers=headers, json=payload)

        if response.status_code == 200:
            print(f"Data successfully sent to Kinesis stream for {partition_key}.")
        else:
            print(f"Error sending data for {partition_key}. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print(f"Error sending data for {partition_key}: {e}")


def kinesis_stream_post(pin_result, geo_result, user_result):
       """
    Posts data to Kinesis streams for pin, geo, and user topics.

    Args:
        pin_result (dict): The data for the pin topic.
        geo_result (dict): The data for the geo topic.
        user_result (dict): The data for the user topic.

    Returns:
        None
    """
    try:
        # Post data to Kinesis for each topic
        api_send_to_kinesis(create_invoke_url(base_url, stream_name, "pin"), headers, pin_result, "pin", stream_name)
        api_send_to_kinesis(create_invoke_url(base_url, stream_name, "geo"), headers, geo_result, "geo", stream_name)
        api_send_to_kinesis(create_invoke_url(base_url, stream_name, "user"), headers, user_result, "user", stream_name)

    except Exception as e:
        print(f"Error posting data to Kinesis streams: {e}")
