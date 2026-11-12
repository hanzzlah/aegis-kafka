import os
import json
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from confluent_kafka import Producer
load_dotenv()
app = FastAPI()


cert_paths = {
    'ca.pem': '/tmp/ca.pem',
    'service.cert': '/tmp/service.cert',
    'service.key': '/tmp/service.key'
}

for env_var, file_path in cert_paths.items():
    cert_content = os.getenv(env_var.replace('.', '_').upper()) 
    if cert_content:
        with open(file_path, 'w') as f:
            f.write(cert_content + '\n')

kafka_config = {
    'bootstrap.servers': os.getenv('AIVEN_URI'), 
    'security.protocol': 'SSL',
    'ssl.ca.location': '/tmp/ca.pem',
    'ssl.certificate.location': '/tmp/service.cert',
    'ssl.key.location': '/tmp/service.key'
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