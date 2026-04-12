from kafka import KafkaConsumer, conn
import snowflake.connector
import json

consumer = KafkaConsumer(
    'sales_topic',
    bootstrap_servers='localhost:29092',  # use working port
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

conn = snowflake.connector.connect(
    user='YOUR_USERNAME',
    password='YOUR_PASSWORD',
    account='YOUR_ACCOUNT',
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