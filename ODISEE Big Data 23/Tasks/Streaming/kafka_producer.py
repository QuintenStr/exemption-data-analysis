# Kafka producer streaming book line by line

import time
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic

bookstream_topicname = 'BookStream'
bootstrap_servers = 'localhost:9092'
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

# probeer te deleten, als dat failed bestaat hij niet dus is ok, anders delete hij sws het topic
topic_names = admin_client.list_topics()
if topic_names:
    for topic_name in topic_names:
        if topic_name == bookstream_topicname:
            print('Found an instance of BookStream topic. deleting...')
            admin_client.delete_topics(topics=[bookstream_topicname])
            
# bugfix met een delay
time.sleep(3)

# maak daarna opnieuw een aan
new_topic = NewTopic(bookstream_topicname, num_partitions=1, replication_factor=1)
admin_client.create_topics(new_topics=[new_topic])
print(f'BookStream topic made.')

# bugfix met een delay
time.sleep(3)

print(f'Beginning streaming line per line.')

# lijn per lijn met 1 seconde delay versturen
file_path = './bookstream.txt'
with open(file_path, "r") as file:
    lines = file.readlines()

for line in lines:
    line = line.strip()
    if line:
        print(bookstream_topicname, line.encode())
        producer.send(bookstream_topicname, line.encode())
        time.sleep(1)

producer.close()
