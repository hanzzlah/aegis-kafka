import os
from fastapi import FastAPI, Request
from confluent_kafka import Producer
import json

app = FastAPI()

cert_paths = {
    'CA_PEM': '/tmp/ca.pem',
    'SERVICE_CERT': '/tmp/service.cert',
    'SERVICE_KEY': '/tmp/service.key'
}

for env_var, file_path in cert_paths.items():
    cert_content = os.getenv(env_var)
    if not cert_content:
        raise ValueError(f"CRITICAL: Environment variable {env_var} is completely missing!")
        
    clean_content = cert_content.replace('\\n', '\n')
    with open(file_path, 'w') as f:
        f.write(clean_content + '\n')

kafka_config = {
    'bootstrap.servers': os.getenv('AIVEN_KAFKA_URI'), 
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