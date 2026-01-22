import os
import json
import snowflake.connector
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
KAFKA_TOPIC = 'twitch_aoe2_stream_data'
BATCH_SIZE = 10  # Insert into Snowflake every 10 messages (keeps it efficient)

# Kafka Config
kafka_conf = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("KAFKA_KEY"),
    'sasl.password': os.getenv("KAFKA_SECRET"),
    'group.id': 'snowflake-consumer-group', # Important for tracking what you've read
    'auto.offset.reset': 'earliest' # Read from the beginning if we missed anything
}

def get_snowflake_conn():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse='COMPUTE_WH',
        database='TWITCH_ANALYTICS',
        schema='RAW_DATA'
    )

def insert_to_snowflake(messages, conn):
    """
    Inserts a list of JSON strings into Snowflake VARIANT column.
    """
    cursor = conn.cursor()
    try:
        # Snowflake expects a list of tuples for executemany
        # We wrap the JSON string in a tuple: (json_str,)
        data_to_insert = [(msg,) for msg in messages]
        
        # The SQL query uses PARSE_JSON to convert string -> Variant
        query = "INSERT INTO STREAM_LOGS (raw_data) SELECT PARSE_JSON($1) FROM VALUES (%s)"
        
        cursor.executemany(query, data_to_insert)
        conn.commit()
        print(f"❄️  Saved {len(messages)} records to Snowflake.")
        
    except Exception as e:
        print(f"Snowflake Error: {e}")
    finally:
        cursor.close()

def consume_loop():
    consumer = Consumer(kafka_conf)
    consumer.subscribe([KAFKA_TOPIC])
    
    conn = get_snowflake_conn()
    print("Listening to Kafka... (Waiting for batch)")
    
    buffer = []

    try:
        while True:
            msg = consumer.poll(1.0) # Wait 1 second for a message

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            # Add valid message value to our buffer
            val = msg.value().decode('utf-8')
            buffer.append(val)

            # If buffer is full, write to Snowflake
            if len(buffer) >= BATCH_SIZE:
                insert_to_snowflake(buffer, conn)
                buffer = [] # Clear buffer

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        # Don't lose the last few messages in the buffer!
        if buffer:
            insert_to_snowflake(buffer, conn)
        consumer.close()
        conn.close()

if __name__ == "__main__":
    consume_loop()
