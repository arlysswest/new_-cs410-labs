from google.cloud import pubsub_v1
from google.oauth2 import service_account
import time

# Define your project and subscription ID
project_id = "your-project-id"
subscription_id = "your-subscription-id"

# Define the path to your service account key file
key_path = "/Users/arlysswest/Desktop/cs-410/data transport/data transport lab/data-engineering-spring-a049302cdbbf.json"

# Create credentials using your service account key file
credentials = service_account.Credentials.from_service_account_file(key_path)

# Create a subscriber client using the credentials
subscriber = pubsub_v1.SubscriberClient(credentials=credentials)

# Create the subscription path
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# Initialize message count
message_count = 0

# Callback function to handle received messages
def callback(message):
    global message_count
    message.ack()  # Acknowledge the message
    message_count += 1  # Increment the message count

# Start timing the consumer process
start_time = time.time()

# Subscribe to the subscription and process messages
future = subscriber.subscribe(subscription_path, callback=callback)
print("Consumer started, processing messages...")

# Keep the subscriber running for a set amount of time (e.g., 30 seconds)
try:
    time.sleep(30)  # Adjust based on how much data you expect
finally:
    # Cancel the future to stop listening
    future.cancel()

    # Print the results after processing
    print(f"✅ Consumer finished.")
    print(f"Total messages received: {message_count}")
    print(f"⏱️ Time taken: {time.time() - start_time:.2f} seconds.")
