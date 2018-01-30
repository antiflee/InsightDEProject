"""
    Subscribe three topics in Kafka: hero-win-rate, hero-pair-win-rate, and
    hero-counter-pair-win-rate. Send the messages to Redis, for the front-end
    to use.
"""

import threading
import redis


#KAFKA_URL = os.getenv("KAFKA_URL")
#REDIS_URL = os.getenv("REDIS_URL")

REDIS_URL = "ec2-34-213-4-249.us-west-2.compute.amazonaws.com"
KAFKA_URL = "ec2-52-41-196-130.us-west-2.compute.amazonaws.com"

# Redis server
r = redis.StrictRedis(host=REDIS_URL, port=6379, db=0)

class kafkaToRedisConsumerThread (threading.Thread):

    def __init__(self, threadID, name, topic_name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.topic_name = topic_name
  
    def run(self):
        
        print("Start consumer thread for topic " + self.topic_name + "\n")
        from kafka import KafkaConsumer
        
        consumer = KafkaConsumer(self.topic_name,
                         group_id='python-redis-'+self.topic_name,
                         bootstrap_servers=[KAFKA_URL+":9092"])
        for message in consumer:
            
            res = message.value.decode('utf-8').split(",")
            
            
            if self.topic_name == "hero-win-rate":
                if len(res) == 2:
                    r.set(res[0], res[1])
            
            elif self.topic_name == "hero-pair-win-rate":
                if len(res) == 3:
                    hero_id1,hero_id2, win_rate = res[0], res[1], res[2]
                    r.set(hero_id1+","+hero_id2, win_rate)
            
            elif self.topic_name == "hero-counter-pair-win-rate":
                if len(res) == 4:
                    _,hero_id1,hero_id2, win_rate = res[0], res[1], res[2], res[3]
                    r.set("counter,"+hero_id1+","+hero_id2, win_rate)

            else:
                print("Valid topic names: (1) hero-win-rate; " +
                      "(2) hero-pair-win-rate; (3) hero-counter-pair-win-rate.")
       
        KafkaConsumer(auto_offset_reset='latest', enable_auto_commit=False)
       
       
def start_three_consumers():
    
    
    # Create new threads
    thread1 = kafkaToRedisConsumerThread(1, "Thread_Single_Hero", "hero-win-rate")
    thread2 = kafkaToRedisConsumerThread(2, "Thread_Hero_Pair", "hero-pair-win-rate")
    thread3 = kafkaToRedisConsumerThread(3, "Thread_Counter_Pair", "hero-counter-pair-win-rate")
    
    # Start the threads
    thread1.start()
    thread2.start()
    thread3.start()


if __name__ == "__main__":
    start_three_consumers()
    