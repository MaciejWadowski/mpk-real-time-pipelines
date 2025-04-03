import datetime

import requests
import gtfs_realtime_pb2 as pb
from google.protobuf.json_format import MessageToJson
import boto3
import json
from datetime import date


def lambda_handler(event, lambda_context):
    url = "https://gtfs.ztp.krakow.pl/TripUpdates_T.pb"
    response = requests.get(url)
    if response.ok:
        feed = pb.FeedMessage()
        feed.ParseFromString(response.content)
        message = MessageToJson(feed)
        json_data = json.dumps(message, separators=(',', ':'))
        client = boto3.client('s3')
        timestamp = datetime.datetime.now().timestamp()
        client.put_object(
            Bucket='somerandometestbucket12312312',
            Key=f"raw/TripUpdates_T/TripUpdates_T_{timestamp}.json",
            Body=json_data,
            ContentType='application/json'
        )
        return f"raw/TripUpdates_T/TripUpdates_T_{timestamp}.json"
    else:
        response.raise_for_status()