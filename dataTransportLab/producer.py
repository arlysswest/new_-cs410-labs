import json
import time
from google.cloud import pubsub_v1
import os

# Setup
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="/Users/arlysswest/Desktop/data-engineering-spring-a049302cdbbf.json"
project_id = "data-engineering-spring"  
topic_id = "MyTopic"
subscription_id= "MySub"
#file_path = "one_day_data.jsonl"
file_path="test.json"
#file_path = "one_day_data.json"
#file_path="bcsample.json"
#file_path="/Users/arlysswest/Desktop/cs-410/new_repo/dataTransportLab/vehicle_data.json"
# Initialize the Pub/Sub Publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Start timing to measure how long it takes to publish messages
start_time = time.time()

# Open the file containing the data and publish each line as a message
message_count = 0  # To track how many messages are published

with open(file_path, 'r') as f:
    for line in f:
        line = line.strip()  # Clean any extra spaces or newlines
        if line:
            try:
                # Publish the message (the line of data) to Pub/Sub
                future = publisher.publish(topic_path, line.encode("utf-8"))
                future.result()  # Wait for message to be successfully published
                message_count += 1  # Increment the message count
            except Exception as e:
                print(f"Failed to publish message: {e}")

# End timing
end_time = time.time()

# Output the result
print(f"Producer published {message_count} messages.")
print(f"Producer completed in {end_time - start_time:.2f} seconds.")

'''
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Start timing
start_time = time.time()

# Send each message
with open(file_path, 'r') as f:
    for line in f:
        line = line.strip()
        if line:
            publisher.publish(topic_path, line.encode("utf-8"))

end_time = time.time()
print(f"Producer completed in {end_time - start_time:.2f} seconds.")
'''