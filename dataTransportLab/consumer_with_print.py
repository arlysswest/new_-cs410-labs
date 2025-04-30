import time
from google.cloud import pubsub_v1
#for debugging
import os

# Setup
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="/Users/arlysswest/Desktop/data-engineering-spring-a049302cdbbf.json"
project_id = "data-engineering-spring"  
topic_id = "MyTopic"
subscription_id= "MySub"
#file_path = "one_day_data.jsonl"
file_path="one_day_data.json"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

count = 0
start_time = time.time()

def callback(message):
    global count
    print(f"Received: {message.data.decode('utf-8')}")
    count += 1
    message.ack()

# Subscribe
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print("Listening for messages with print...")

try:
    streaming_pull_future.result(timeout=30)  # adjust timeout as needed
except Exception as e:
    streaming_pull_future.cancel()
    end_time = time.time()
    print(f"Consumer with print received {count} messages in {end_time - start_time:.2f} seconds.")
