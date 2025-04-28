#needs google cloud credentials
from google.cloud import pubsub_v1
import json

project_id = "your-project-id"
topic_id = "my-topic"

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
