from kafka import KafkaConsumer
from json import loads
from pymongo import MongoClient
import time


time.sleep(122)
while True:
    try:
        moviecons = KafkaConsumer('movieapi',bootstrap_servers=['broker:29092'],value_deserializer=lambda x: loads(x.decode('utf-8')))
        break
    except:
        print('Waiting for KafkaBroker')
        time.sleep(60)
        print('Next try!')
    


cluster = MongoClient(
    "mongodb+srv://setup_admin:BIgQIsyGh3wt1Hrl@kafkaproject.ip0ti.mongodb.net/nytimes?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE")
print("successfully connected to mongodb")
db = cluster["projectdb"]
collection = db["nowplayingmovies"]
for message in moviecons:
    # print(message.value)
    # print(message)
    collection.insert_one(message.value)
