#generate pseudo-random collection for a discogs user and post to kafka topic

from kafka import KafkaProducer
import random
import json

producer=KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('ascii'))
user_counter=1
while 1==1:
  seed=(random.randint(1,10100000))
  offset=(random.randint(0,200))
  user_id='user' + str(user_counter)
  d=dict()
  l=list()
  for i in range ((seed),(seed+offset)):
    l.append(i)
  d["user_id"]=user_id
  d["collection"]=l
  user_counter += 1

  #print(d)
  producer.send("user-collection-stream",d)
  if user_counter % 10000==0:
    print "generated ",user_counter," users"
