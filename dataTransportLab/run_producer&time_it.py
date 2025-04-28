#needs google cloud credentia;s
import json, time
from google.cloud import pubsub_v1
import os
#code errors
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="/Users/arlysswest/Desktop/data-engineering-spring-a049302cdbbf.json"


publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("your-project-id", "my-topic")

with open("breadcrumbs_full_day.json", "r") as f:
    data = json.load(f)

start = time.time()
for record in data:
    publisher.publish(topic_path, data=json.dumps(record).encode("utf-8"))
print(f"✅ Producer sent {len(data)} messages in {time.time() - start:.2f} seconds.")
