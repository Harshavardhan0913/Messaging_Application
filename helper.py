import json
import redis

class RedisClient:
    def __init__(self, host="localhost", port=6379, db=0):
        self.redis_client = self._connectRedis(host, port, db)
    
    def _connectRedis(self, host, port, db):
        """
        host: hostname of the redis server
        port: port number on which redis server is running
        db: number the database that needs to be accessed [default dbs are 16]
        """
        return redis.Redis(host=host, port=port, db=db)
    
    def setKey(self, key, value):
        self.redis_client.set(key, value)
    
    def getKey(self, key):
        return self.redis_client.get(key).decode('utf-8')

class MessageApplication:
    def __init__(self, redis_client):
        self.redis_client = redis_client
    
    def publish_message(self, message, channel):
        try:
            self.redis_client.publish(channel, json.dumps(message))
            self.redis_client.lpush(f"chat:{channel}", json.dumps(message))
            self.redis_client.ltrim(f"chat:{channel}", 0, 99)
        except Exception:
            return "Failed to publish Message"
        return "Successfully published Message"
    
    def get_messages(self, channel):
        data = self.redis_client.lrange(f"chat:{channel}", 0, -1)
        messages = []
        for message in data:
            messages.append(json.loads(message))
        return messages
    
    def get_latest_message(self, channel):
        return self.get_messages(channel)[:1]
    
def print_data(redis_client):
    data = {
        "name": "Harsha",
        "profession": "Software Engineer",
        "age": 23
    }
    for key, value in data.items():
        redis_client.setKey(key, value)
    
    for key in data.keys():
        print(key,":", redis_client.getKey(key))

def subscribe():
    redis_client = redis.Redis(host="localhost", port=6379, db=0)
    channel = "test_channel"
    pub_sub = redis_client.pubsub()
    pub_sub.subscribe(channel)
    print(f"Subscribed to {channel}, waiting for messages")
    for message in pub_sub.listen():
        if message['type']=='message':
            receive_message = json.loads(message['data'].decode('utf-8'))
            print(f"From: {receive_message['sender']}\nMessage: {receive_message['message']}\n")

