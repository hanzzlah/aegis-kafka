import os
import json
import uuid
import random
import asyncio

from typing import Any, cast
from dotenv import load_dotenv
from confluent_kafka import Producer
from datetime import datetime, timezone
from supabase import create_client, Client

load_dotenv()

supabase: Client = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

producer = Producer({
    'bootstrap.servers': os.getenv('AIVEN_URI'),
    'security.protocol': 'SSL',
    'ssl.ca.location': 'ca.pem',
    'ssl.certificate.location': 'service.cert',
    'ssl.key.location': 'service.key'
})

def get_real_users():
    response = supabase.table("user_profiles").select("user_id, trust_score").execute()
    users = cast(list[dict[str, Any]], response.data)
    healthy = [u['user_id'] for u in users if u['trust_score'] > 0.5]
    sketchy = [u['user_id'] for u in users if u['trust_score'] <= 0.5]
    
    if not sketchy and healthy:
        sketchy = random.sample(healthy, max(1, int(len(healthy) * 0.2)))
        healthy = list(set(healthy) - set(sketchy))
    return healthy, sketchy

async def trigger_bot_burst(sketchy_users, post_id):
    print(f"\n🚨 BOT SWARM ON {post_id}!")
    ts = datetime.now(timezone.utc).isoformat()
    for bot in sketchy_users:
        event = {"interaction_id": str(uuid.uuid4()), "user_id": bot, "post_id": post_id, "interaction_type": "like", "timestamp": ts}
        producer.produce('telemetry_interactions', json.dumps(event).encode('utf-8'))
    producer.flush()

async def continuous_traffic():
    healthy, sketchy = get_real_users()
    posts = [f"post_{i}" for i in range(1, 11)]
    event_count = 0 # Initialize counter
    print("🌊 Stream Started. Simulating Poisson distribution...")
    
    try:
        while True:
            await asyncio.sleep(random.expovariate(0.5)) 
            
            if healthy:
                event = {"interaction_id": str(uuid.uuid4()), "user_id": random.choice(healthy), "post_id": random.choice(posts), "interaction_type": random.choices(['like', 'view', 'comment', 'share'], weights=[40, 40, 10, 10])[0], "timestamp": datetime.now(timezone.utc).isoformat()}
                producer.produce('telemetry_interactions', json.dumps(event).encode('utf-8'))
                producer.poll(0)
                event_count += 1 # Increment counter
                print(f"💧 {event['interaction_type']} by {event['user_id'][:8]} | Total: {event_count}")
            
            if random.random() < 0.02 and sketchy:
                await trigger_bot_burst(sketchy, random.choice(posts))

    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\n🛑 Interrupted by user")
    finally:
        print("⏳ Flushing producer...")
        producer.flush()
        print(f"💾 Total events produced: {event_count}")
        print("✅ Shutdown complete.")

if __name__ == "__main__":
    asyncio.run(continuous_traffic())