"""
    This scripts is on the web-cluster node. It reads 3 topics from Kafka:
        hero-win-rate, hero-pair-win-rate, hero-counter-pair-win-rate
    Then it sends the results to the redis server.
"""
import os

import redis
from kafka import KafkaConsumer

#KAFKA_URL = os.getenv("KAFKA_URL")
#REDIS_URL = os.getenv("REDIS_URL")

REDIS_URL = "ec2-34-213-4-249.us-west-2.compute.amazonaws.com"
KAFKA_URL = "ec2-52-41-196-130.us-west-2.compute.amazonaws.com"

# Redis server
r = redis.StrictRedis(host=REDIS_URL, port=6379, db=0)

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('hero-win-rate',
                         group_id='python-redis-hero-win-rate',
                         bootstrap_servers=[KAFKA_URL+":9092"])
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
#    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
#                                          message.offset, message.key,
#                                          message.value))
    hero_id, win_rate = message.value.decode('utf-8').split(",")
    r.set(hero_id, win_rate)

# consume earliest available messages, don't commit offsets
#KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)