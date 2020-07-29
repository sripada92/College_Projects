import json
import requests
import time
from kafka import KafkaProducer



time.sleep(125)


 
while True:
	try:
		now_playing = requests.get('https://api.themoviedb.org/3/movie/now_playing?api_key=dfd8fe4364947db1f86bb33a39ecd6bf&language=en-US&page=1')
		now_playing_load = now_playing.json()
		producer = KafkaProducer(bootstrap_servers=['broker:29092'], acks=1,value_serializer=lambda v: json.dumps(v).encode('utf-8'))
		producer.send('movieapi',value=now_playing_load)
		time.sleep(10)
	except:
		time.sleep(60)
		print('Next try!')