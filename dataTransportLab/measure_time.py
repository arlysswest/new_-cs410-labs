import json
import time
# from google.cloud import pubsub_v1 (if using GCP)

start_time = time.time()
with open('one_day_data.jsonl', 'r') as f:
    for line in f:
        message = line.strip()
        if message:
            # publisher.publish(topic_path, message.encode("utf-8"))
            pass  # Replace with actual publish call
end_time = time.time()

print(f"Producer completed in {end_time - start_time:.2f} seconds.")
