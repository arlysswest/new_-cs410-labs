#needs google cloud credentia;s
import json, time
from google.cloud import pubsub_v1
import os
#code errors
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="/Users/arlysswest/Desktop/data-engineering-spring-a049302cdbbf.json"
project_id = "data-engineering-spring"  
topic_id = "MyTopic"
subscription_id= "MySub"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("your-project-id", "my-topic")

#with open("breadcrumbs_full_day.json", "r") as f:
#    data = json.load(f)
'''
data = []
with open('breadcrumbs_full_day.json', 'r') as f:
    for line in f:
        line = line.strip()
        if line:
            try:
                data.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"Skipping bad line: {e}")
'''
with open("breadcrumbs_full_day.json", "w") as f:
    json.dump(all_data, f, indent=2)

start = time.time()
for record in data:
    publisher.publish(topic_path, data=json.dumps(record).encode("utf-8"))
print(f"✅ Producer sent {len(data)} messages in {time.time() - start:.2f} seconds.")
