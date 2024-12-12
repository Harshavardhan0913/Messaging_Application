import fastapi
import redis
import threading
import uvicorn
from helper import MessageApplication, subscribe
from models import Message

app = fastapi.FastAPI()
redis_client = redis.Redis(host="localhost", port=6379, db=0)

@app.post('/send_message')
def send_message(message: Message):
    channel = message.channel
    messageApplication = MessageApplication(redis_client)
    sender_message = {
        "sender" : message.sender,
        "message" : message.message,
    }
    return messageApplication.publish_message(sender_message, channel)

@app.get("/get_messages")
def get_messages(channel):
    messageApplication = MessageApplication(redis_client)
    data = messageApplication.get_messages(channel)
    return data

@app.get('/get_latest_message')
def get_latest_message(channel):
    messageApplication = MessageApplication(redis_client)
    data = messageApplication.get_latest_message(channel)
    return data

if __name__ == '__main__':
    subscribe_thread = threading.Thread(target=subscribe, daemon=True)
    subscribe_thread.start()

    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="info")
