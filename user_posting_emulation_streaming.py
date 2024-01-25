from user_posting_emulation import *


base_url = "https://amdpzoul4j.execute-api.us-east-1.amazonaws.com/test"
stream_name = "streaming-12a3da8f7ced"


headers = {'Content-Type': 'application/json'}
topics = ["pin", "geo", "user"]


def create_invoke_url(base_url, stream_name, topic):
    return f"{base_url}/streams/{stream_name}-{topic}/record"


def api_send_to_kinesis(url, headers, table_dict, partition_key):
    try:
        payload = {
            "records": [
                {
                    "value": table_dict
                }
            ]
        }
        response = requests.post(url, headers=headers, json=payload)

        
        if response.status_code == 200:
            print(f"Data successfully sent to Kinesis stream for {partition_key}.")
        else:
            print(f"Error sending data for {partition_key}. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print(f"Error sending data for {partition_key}: {e}")


def kinesis_stream_post(pin_result, geo_result, user_result):
    try:
        api_send_to_kinesis(create_invoke_url(base_url, stream_name, "pin"), headers, pin_result, "pin")
        api_send_to_kinesis(create_invoke_url(base_url, stream_name, "geo"), headers, geo_result, "geo")
        api_send_to_kinesis(create_invoke_url(base_url, stream_name, "user"), headers, user_result, "user")
    except Exception as e:
        print(f"Error posting data to Kinesis streams: {e}")

