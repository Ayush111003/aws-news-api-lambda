import json
import os
import random
import urllib.request
import urllib.error
import boto3
from datetime import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Load environment variables
        api_key = os.environ['NEWS_API_KEY']
        news_count = int(os.environ['NEWS_COUNT'])
        topics = os.environ['NEWS_TOPICS'].split(',')
        bucket_name = os.environ['S3_BUCKET']

        # Select random topic
        topic = random.choice(topics)
        print(f"Selected topic: {topic}")

        # Random endpoint selection (1/3 bad, 2/3 good)
        if random.randint(1, 3) == 1:
            endpoint = "https://newsapi.org:444/v2/everything"
        else:
            endpoint = "https://newsapi.org/v2/everything"
        print(f"Using endpoint: {endpoint}")

        url = f"{endpoint}?q={topic}&pageSize={news_count}"
        req = urllib.request.Request(url)
        req.add_header("X-Api-Key", api_key)
        response = urllib.request.urlopen(req, timeout=3)
        status_code = response.getcode()

        if status_code == 200:
            data = response.read()
            timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
            key = f"news/{topic}/{timestamp}.json"
            s3.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=data,
                ContentType="application/json"
            )
            print("News stored successfully")
            return {"statusCode": 200, "body": "Success"}
        else:
            print(f"Non-200 status received: {status_code}")
            return {"statusCode": status_code}

    except urllib.error.URLError as e:
        print(f"Network error occurred: {str(e)}")
        return {"statusCode": 500, "body": "Networklets failure handled"}

    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return {"statusCode": 500, "body": "Execution failed safely"}
