import requests
import os
import time

loki_push_url = os.environ.get("LOKI_ENDPOINT") + "/loki/api/v1/push"
def send_log_to_loki(log_message: str, labels: dict, timestamp=None):
    """
    Sends a log message to the Loki endpoint.

    :param labels: A dictionary of labels for the log stream.
    :param log_message: The log message to send.
    :param timestamp: The timestamp (epoch format in nanoseconds) of the log message. If None, the current time is used.
    """
    headers = {
        'Content-Type': 'application/json',
    }
    if timestamp is None:
        timestamp =str(int(time.time() * 1e9))

    log_entry = {
        "streams": [
            {
                "stream": labels,
                "values": [
                    [timestamp, f"{log_message}"]
                ]
            }
        ]
    }


    response = requests.post(loki_push_url, headers=headers, json=log_entry)
    print("loki-logs",log_entry)

    if response.status_code != 204:
        print(f"Failed to send log to Loki: {response.status_code} {response.text}")
    else:
        print("Log sent successfully")