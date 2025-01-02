import redis
import json

print ("Redis Subscriber")

redis_conn = redis.Redis(
    host='localhost',
    port=6379,
    decode_responses=True)

def sub():
    pubsub = redis_conn.pubsub()
    pubsub.subscribe("task")
    for message in pubsub.listen():
        if message.get("type") == "message":
            data = message.get("data")
            print(data)

if __name__ == "__main__":
    sub()