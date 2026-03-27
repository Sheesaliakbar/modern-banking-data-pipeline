import json
import logging
from datetime import datetime, timedelta
from io import BytesIO
import pandas as pd
from kafka import KafkaConsumer
import boto3
from botocore.client import Config

# 1. Logging Setup
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BankingPipeline:
    def __init__(self):
        # Configuration
        self.kafka_topic_pattern = 'banking.public.*' 
        self.bootstrap_servers = ['localhost:9092']
        self.bucket_name = 'rawbanking'
        
        # MinIO Setup (S3 Compatible)
        self.s3 = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            # Windows compatibility fix for MinIO paths
            config=Config(signature_version='s3v4', s3={'addressing_style': 'path'})
        )

    def format_date(self, days):
        """Postgres epoch integers ko human-readable date mein convert karta hai"""
        if days is None: return None
        try:
            return (datetime(1970, 1, 1) + timedelta(days=days)).strftime('%Y-%m-%d')
        except:
            return str(days)

    def run(self):
        logger.info("🚀 Connecting to Kafka...")
        
        # Initialize Consumer with a new group_id to avoid offset issues
        consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='minio_banking_final_v1'
        )
        
        # Multiple topics ko subscribe karne ke liye pattern use kiya hai
        consumer.subscribe(pattern=self.kafka_topic_pattern)

        logger.info(f"📡 Pipeline Started! Listening to: {self.kafka_topic_pattern}")

        for message in consumer:
            try:
                # 1. Table ka naam topic se nikaalein
                topic_name = message.topic
                table_name = topic_name.split('.')[-1] 

                # 2. Debezium payload extract karein
                payload = message.value.get('payload', message.value)
                data = payload.get('after')
                
                if not data:
                    continue

                # 3. Data Cleaning & Type Casting Fix
                clean_data = {}
                for key, value in data.items():
                    # Dates handle karein
                    if 'date' in key or 'birth' in key:
                        clean_data[key] = self.format_date(value)
                    else:
                        clean_data[key] = value
                
                clean_data['processed_at'] = datetime.now().isoformat()

                # 4. DataFrame Fix: Python int too large error ke liye
                df = pd.DataFrame([clean_data])
                
                # Sabhi integers ko 64-bit mein convert karein taake Parquet file crash na ho
                for col in df.select_dtypes(include=['int', 'int64']).columns:
                    df[col] = df[col].astype('int64')

                # 5. Convert to Parquet (In-Memory)
                buffer = BytesIO()
                df.to_parquet(
                    buffer, 
                    index=False, 
                    engine='pyarrow', 
                    coerce_timestamps='ms', 
                    allow_truncated_timestamps=True
                )
                buffer.seek(0)

                # 6. Dynamic Path Generation (Table wise folders)
                file_time = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
                s3_key = f"{table_name}/year={datetime.now().year}/month={datetime.now().month}/{table_name}_{file_time}.parquet"

                # 7. Upload to MinIO
                self.s3.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=buffer.getvalue()
                )

                logger.info(f"✅ SUCCESS: Saved record to {table_name} folder in MinIO")

            except Exception as e:
                logger.error(f"❌ Error processing record from {message.topic}: {e}")

if __name__ == "__main__":
    pipeline = BankingPipeline()
    pipeline.run()