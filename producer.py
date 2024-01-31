from kafka import KafkaProducer
import json
import pandas as pd

data = pd.read_csv("offline.csv")

bootstrap_servers = 'localhost:9092'
topic = 'health_data'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

for _, row in data.iterrows():
    row_data = row.drop('Diabetes_binary').to_dict()
    producer.send(topic, value=row_data)

producer.close()
