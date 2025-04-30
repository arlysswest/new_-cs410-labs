from google.cloud import pubsub_v1
import time
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="/Users/arlysswest/Desktop/data-engineering-spring-a049302cdbbf.json"

project_id = "data-engineering-spring"  
topic_id = "MyTopic"
subscription_id= "MySub"
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

message_count = 0

def callback(message):
    global message_count
    message.ack()
    message_count += 1

start_time = time.time()
future = subscriber.subscribe(subscription_path, callback=callback)
print("Receiving messages...")

try:
    time.sleep(15)  # Adjust based on volume
finally:
    future.cancel()
    print(f"⏱️ Received {message_count} messages in {time.time() - start_time:.2f} seconds.")
