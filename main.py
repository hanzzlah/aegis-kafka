import os
import json
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from confluent_kafka import Producer
load_dotenv()
app = FastAPI()

kafka_config = {
    'bootstrap.servers': os.getenv('AIVEN_URI'), 
    'security.protocol': 'SSL',
    'ssl.ca.location': 'ca.pem',
    'ssl.certificate.location': 'service.cert',
    'ssl.key.location': 'service.key'
}

producer = Producer(kafka_config)

@app.post("/aegis-cdc")
async def receive_supabase_webhook(request: Request):
    cdc_payload = await request.json()
    producer.produce(
        topic='cdc_profiles',
        value=json.dumps(cdc_payload).encode('utf-8')
    )
    producer.flush()
    return {"message": "CDC event published to Aiven Kafka"}