from google.cloud import pubsub_v1
import json
import os

#code errors
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="/Users/arlysswest/Desktop/data-engineering-spring-a049302cdbbf.json"
project_id = "data-engineering-spring"  
topic_id = "MyTopic"
subscription_id= "MySub"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Load the sample data
with open("bcsample.json", "r") as f:
    records = json.load(f)

for i in range(5):  # Run this loop or script 5 separate times to simulate backlog
    for record in records:
        data = json.dumps(record).encode("utf-8")
        publisher.publish(topic_path, data=data)
    print(f"Published {len(records)} messages in run {i+1}.")
