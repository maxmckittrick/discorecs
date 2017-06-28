#generate pseudo-random wantlist for a discogs user and post to kafka topic

from kafka import KafkaProducer
import random
import json

producer=KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('ascii'))
user_counter=1
while 1==1:
  seed=(random.randint(0,33667)) #subset of the total volume of discogs releases w/ complete metadata
  offset_seed=(random.randint(0,100))
  user_id='user'+str(user_counter) #user IDs will be in the form userXXXX
  user_id_kv=dict()
  user_wantlist=list()
  for i in range ((seed),(seed+150-offset_seed)):
    user_wantlist.append(i) #each simulated user will have 150-50 releases in their wantlist
  user_id_kv[0]=user_id
  user_id_kv[1]=user_wantlist
  user_counter += 1
  if user_counter%10000==0:
    print(str(user_counter)+" users sent to topic user_wantlist_stream")
    producer.send("user_wantlist_stream",user_id_kv)
