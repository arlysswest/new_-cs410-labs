#needs google cloud credentials
from google.cloud import pubsub_v1
import time
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="/Users/arlysswest/Desktop/data-engineering-spring-a049302cdbbf.json"


subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path("your-project-id", "my-sub")

def callback(message):
    print(f"Received: {message.data}")  # Debug print
    message.ack()

start_time = time.time()
future = subscriber.subscribe(subscription_path, callback=callback)
print("Consumer started with print()...")

try:
    time.sleep(30)
finally:
    future.cancel()
    print(f"⏱️ Consumer with print() finished in {time.time() - start_time:.2f} seconds.")
