from fastapi import FastAPI
import pika
import random
import time
from datetime import datetime
import json
import asyncio

app = FastAPI()

NEWS_SCHEMA = {
    "categories": ["Technology", "Business", "World", "Science"],
    "keywords": {
        "Technology": ["AI", "Blockchain", "Cybersecurity", "Cloud", "5G", "IoT"],
        "Business": ["Stocks", "Economy", "Startup", "Market", "Investment", "Trade"],
        "World": ["Politics", "Climate", "Diplomacy", "International", "Summit", "Treaty"],
        "Science": ["Research", "Discovery", "Space", "Medicine", "Physics", "Biology"]
    },
    "templates": {
        "Technology": {
            "titles": [
                "New breakthrough in {tech}",
                "Major company announces {tech} innovation",
                "Revolutionary {tech} development"
            ],
            "contents": [
                "Scientists have made a breakthrough in {tech} technology that could revolutionize the industry.",
                "A leading tech company has announced new developments in {tech}.",
                "Experts predict that recent {tech} advances will change the future."
            ]
        },
        "Business": {
            "titles": [
                "Market surge in {tech}",
                "New investment opportunity in {tech}",
                "Economic changes affect {tech}"
            ],
            "contents": [
                "Investors are showing increased interest in {tech} markets.",
                "Economic analysts predict growth in {tech} sector.",
                "Major developments in {tech} impact global markets."
            ]
        },
        "World": {
            "titles": [
                "Global summit addresses {tech}",
                "International cooperation on {tech}",
                "World leaders discuss {tech}"
            ],
            "contents": [
                "Nations gather to address {tech} in landmark meeting.",
                "International community focuses on {tech} solutions.",
                "Global initiative launched to tackle {tech}."
            ]
        },
        "Science": {
            "titles": [
                "Scientists discover new {tech}",
                "Breakthrough in {tech} research",
                "Revolutionary {tech} findings"
            ],
            "contents": [
                "Research team announces major discovery in {tech}.",
                "New study reveals breakthrough in {tech} understanding.",
                "Scientific community excited about {tech} developments."
            ]
        }
    }
}

def generate_news_item():
    category = random.choice(NEWS_SCHEMA["categories"])
    keywords = random.sample(NEWS_SCHEMA["keywords"][category], k=random.randint(2, 4))
    
    template = NEWS_SCHEMA["templates"][category]
    title = random.choice(template["titles"]).format(tech=random.choice(keywords))
    content = random.choice(template["contents"]).format(tech=random.choice(keywords))
    
    news_item = {
        "title": title,
        "content": content,
        "category": category,
        "timestamp": datetime.now().isoformat(),
        "keywords": keywords
    }
    
    return news_item

def send_to_rabbitmq(message):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        
        # Declare queue
        channel.queue_declare(
            queue='news_queue',
            durable=False
        )
        
        # Publish message directly to queue
        channel.basic_publish(
            exchange='',  # Use default exchange
            routing_key='news_queue',  # Use queue name as routing key
            body=json.dumps(message),
           
        )
        
        print(f"Published news: {message['title']}")
    except Exception as e:
        print(f"Error publishing message: {e}")
    finally:
        connection.close()

async def news_generator():
    print("News Generator Started...")
    while True:
        news_item = generate_news_item()
        send_to_rabbitmq(news_item)
        await asyncio.sleep(random.uniform(5, 10))

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(news_generator())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
