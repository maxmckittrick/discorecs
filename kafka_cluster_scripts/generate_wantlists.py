#generate pseudo-random wantlist for a discogs user and post to kafka topic (experimental)

from kafka import KafkaProducer
import random
import json

producer=KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('ascii'))
user_counter=1
while 1==1:
  seed=(random.randint(0,33667))
  user_id='user' + str(user_counter)
  d=dict()
  l=list()
  for i in range ((seed),(seed+300)):
     l.append(i)
  d[0]=user_id
  d[1]=l
  user_counter += 1
  #print(d)
  producer.send("user-collections-stream",d)
