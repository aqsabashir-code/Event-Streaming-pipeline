import pandas as pd
from kafka import KafkaProducer
import json
import time

# 1. Kafka Producer Setup
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("✅ Kafka Producer Connected Successfully!")
except Exception as e:
    print(f"❌ Error: Kafka chal raha hai? {e}")
    exit()

# 2. Dataset Load Karein
file_path = 'uae_ecom_fraud_100k.csv' # Make sure ye file usi folder mein ho
df = pd.read_csv(file_path)

print(f"📊 Dataset Loaded: {len(df)} rows found. Starting Stream...")

# 3. Data Streaming Loop
for index, row in df.iterrows():
    # Row ko dictionary mein convert karein
    data = row.to_dict()
    
    # Kafka ko bheinjein
    producer.send('fraud-topic', value=data)
    
    # Console par dikhane ke liye
    print(f"🚀 Sending Transaction ID: {data['transaction_id']} | Amount: {data['amount_aed']} AED")
    
    # Real-time feel ke liye thora sa gap (0.5 second)
    time.sleep(0.5) 

producer.flush()