# working_kafka_to_minio.py
from kafka import KafkaConsumer
import json
import pandas as pd
from io import BytesIO
from datetime import datetime
import boto3
from botocore.client import Config

print("🚀 Starting Kafka to MinIO pipeline...")

# 1. Kafka Consumer
consumer = KafkaConsumer(
    'banking.public.customers',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # Only new messages
    group_id='minio_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# 2. MinIO Client
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    config=Config(signature_version='s3v4'),
    use_ssl=False
)

BUCKET = 'rawbanking'

# 3. Check bucket
try:
    s3.head_bucket(Bucket=BUCKET)
    print(f"✅ MinIO bucket '{BUCKET}' exists")
except:
    s3.create_bucket(Bucket=BUCKET)
    print(f"🪣 Created bucket '{BUCKET}'")

print("\n📝 Ready! Insert data in PostgreSQL to see it flow:")
print("   docker exec postgres psql -U admin -d banking")
print("   INSERT INTO customers (first_name, last_name, email) VALUES ('Test', 'User', 'test@test.com');")
print("\n⏳ Waiting for messages...\n")

try:
    for message in consumer:
        data = message.value
        
        if 'after' in data:
            customer = data['after']
            
            # Convert to DataFrame
            df = pd.DataFrame([customer])
            
            # Save as Parquet in memory
            buffer = BytesIO()
            df.to_parquet(buffer, engine='pyarrow', index=False)
            buffer.seek(0)
            
            # Generate filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"customers/customer_{customer['customer_id']}_{timestamp}.parquet"
            
            # Upload to MinIO
            s3.put_object(
                Bucket=BUCKET,
                Key=filename,
                Body=buffer.getvalue(),
                ContentType='application/parquet'
            )
            
            print(f"✅ Uploaded: {filename}")
            print(f"   Customer: {customer['first_name']} {customer['last_name']}")
            
except KeyboardInterrupt:
    print("\n🛑 Stopping consumer...")
finally:
    consumer.close()