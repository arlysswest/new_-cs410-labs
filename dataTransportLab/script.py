#WORKING!!
from google.cloud import pubsub_v1
import json
import time
import os

# Set path to your credentials (if not set in shell)
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/arlysswest/keys/gcloud-creds.json"
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/arlysswest/Desktop/cs-410/data transport/data transport lab/data-engineering-spring-a049302cdbbf.json"
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/arlysswest/Desktop/cs-410/data transport/data transport lab/data-engineering-spring-a049302cdbbf.json"
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="/Users/arlysswest/Desktop/cs-410/data_transport/data transport lab/data-engineering-spring-a049302cdbbf.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="/Users/arlysswest/Desktop/data-engineering-spring-a049302cdbbf.json"
# Set topic path
project_id = "data-engineering-spring"  
topic_id = "MyTopic"
topic_path = f"projects/{project_id}/topics/{topic_id}"

# Create a publisher client
publisher = pubsub_v1.PublisherClient()

# Load data from file
#with open("bcsample.json") as f:
#with open("vehicle_data.json") as f:
#with open("bcsample.json") as f:
#with open("/Users/arlysswest/Desktop/cs-410/new_repo/dataTransportLab/bcsample.json") as f:
 #   data = json.load(f)
data = []
#with open('/Users/arlysswest/Desktop/cs-410/new_repo/dataTransportLab/bcsample.json', 'r') as f:
with open('/Users/arlysswest/Desktop/cs-410/new_repo/dataTransportLab/vehicle_data.json', 'r') as f:
#    for line in f:
#        data.append(json.loads(line))
    for line in f:
        line = line.strip()
        if not line:  # skip blank lines
            continue
        try:
            data.append(json.loads(line))
        except json.JSONDecodeError as e:
            print(f"Skipping line due to JSON error: {e}")

# Publish each record
for record in data:
    message = json.dumps(record).encode("utf-8")
    future = publisher.publish(topic_path, data=message)
    print(f"Published: {record.get('vehicle_id', 'N/A')} @ {record.get('timestamp', 'N/A')}")

print(f"Published {len(data)} records.")
