# main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pika
import asyncio
from concurrent.futures import ThreadPoolExecutor
import json

app = FastAPI()

origins = ["http://localhost", "http://localhost:8080"]
myDataObject = {
    "name":'ilay', 
    "age": 11
}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def send_to_rabbitmq(message):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    channel.queue_declare(queue='request_queue')
    
    channel.basic_publish(exchange='',
                         routing_key='request_queue',
                         body=message)
    
    connection.close()

def consume_from_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    channel.queue_declare(queue='request_queue')
    
    # Get one message and close the connection
    method_frame, header_frame, body = channel.basic_get(queue='request_queue', auto_ack=True)
    
    connection.close()
    
    if method_frame:
        return body.decode()
    return None

@app.get("/")
async def root():
    # Send message to RabbitMQ
    message = json.dumps(myDataObject)
    send_to_rabbitmq(message)
    
    # Use ThreadPoolExecutor to run the blocking RabbitMQ consumer in a separate thread
    with ThreadPoolExecutor() as executor:
        # Wait a bit to ensure the message is in the queue
        await asyncio.sleep(0.1)
        # Consume the message
        received_message = await asyncio.get_event_loop().run_in_executor(executor, consume_from_rabbitmq)
    
    if received_message:
        return received_message
    return "No message received"
