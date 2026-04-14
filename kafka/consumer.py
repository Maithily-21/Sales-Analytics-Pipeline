from kafka import KafkaConsumer
import snowflake.connector
import json

consumer = KafkaConsumer(
    'sales_topic',
    bootstrap_servers="127.0.0.1:9092",  # Match producer's connection
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

conn = snowflake.connector.connect( 
    user='MAITHILY', 
    password='MaithilyPatle2005', 
    account='vmjyaal-cv32934', 
    warehouse='COMPUTE_WH', 
    database='SUPERSTORE_DB', 
    schema='RAW_DATA' 
    )

cursor = conn.cursor()

for msg in consumer:
    data = msg.value

    query = f"""
    INSERT INTO raw_data.sales_stream_raw(data)
    SELECT PARSE_JSON('{json.dumps(data)}')
    """
    
    cursor.execute(query)
    print("Inserted:", data)