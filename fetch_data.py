import asyncio
import json
import os
from datetime import datetime
from dotenv import load_dotenv

# Twitch Library
from twitchAPI.twitch import Twitch
from twitchAPI.helper import first

# Kafka Library
from confluent_kafka import Producer

load_dotenv()

# --- KAFKA SETUP ---
kafka_conf = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP"),    
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("KAFKA_KEY"),              
    'sasl.password': os.getenv("KAFKA_SECRET"),            
    'client.id': 'aoe2-producer'
}
producer = Producer(kafka_conf)

def delivery_report(err, msg):
    """Callback to confirm message delivery."""
    if err:
        print(f"❌ Message failed: {err}")
    else:
        print(f"✅ Sent: {msg.key().decode('utf-8')}", end=" | ")

async def producer_loop():
    client_id = os.getenv("client_id")
    client_secret = os.getenv("client_secret")
    twitch = await Twitch(client_id, client_secret)
    
    target_game_name = 'Age of Empires II: Definitive Edition'
    game = await first(twitch.get_games(names=[target_game_name]))
    
    if not game:
        print("Could not find game ID. Check spelling.")
        return

    print(f"Target Acquired: {game.name} (ID: {game.id})")
    print("Starting Ingestion Stream... (Press Ctrl+C to stop)")

    while True:
        try:
            streams = twitch.get_streams(game_id=game.id, first=20)
            
            count = 0
            timestamp = datetime.now().isoformat()
            
            async for stream in streams:
                started_at_str = stream.started_at.isoformat() if stream.started_at else None
                data_package = {
                    "timestamp": timestamp,
                    "stream_id": stream.id,
                    "user_name": stream.user_name,
                    "game_name": stream.game_name,
                    "viewer_count": stream.viewer_count,
                    "started_at": started_at_str,
                    "language": stream.language
                }

                producer.produce(
                    topic='twitch_aoe2_stream_data',
                    key=stream.user_name, 
                    value=json.dumps(data_package),
                    on_delivery=delivery_report
                )
                count += 1

            producer.flush()
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Batch complete. Sent {count} records.")

        except Exception as e:
            print(f"Error in producer loop: {e}")

        await asyncio.sleep(60)

if __name__ == "__main__":
    try:
        asyncio.run(producer_loop())
    except KeyboardInterrupt:
        print("Stopping producer...")
