#needs google cloud credentials
from google.cloud import pubsub_v1
import time
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="/Users/arlysswest/Desktop/data-engineering-spring-a049302cdbbf.json"


project_id = "your-project-id"
subscription_id = "my-subscription-id"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

message_count = 0

def callback(message):
    global message_count
    message.ack()
    message_count += 1

start_time = time.time()
future = subscriber.subscribe(subscription_path, callback=callback)
print("Consumer started to clear backlog...")

try:
    time.sleep(30)  # Adjust as needed
finally:
    future.cancel()
    print(f"✅ Consumed {message_count} messages in {time.time() - start_time:.2f} seconds.")
