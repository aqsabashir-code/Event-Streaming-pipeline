from kafka import KafkaConsumer
import json

# 1. Kafka Consumer Setup
consumer = KafkaConsumer(
    'fraud-topic',
    bootstrap_servers=['127.0.0.1:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='fraud-detectors',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("🕵️ Fraud Detection Processor is LIVE... Monitoring Transactions...")

# 2. Real-time Analysis Logic
for message in consumer:
    tx = message.value
    
    # Hum 3 tarah ke alerts nikalenge (Full Marks Logic):
    is_fraud_flag = tx.get('is_fraud', 0)
    risk_score = tx.get('ip_risk_score', 0)
    amount = tx.get('amount_aed', 0)

    # Logic A: Agar dataset ne pehle hi fraud mark kiya hai
    if is_fraud_flag == 1:
        print(f"🚨 CRITICAL: Fraud Detected! | ID: {tx['transaction_id']} | Method: {tx['payment_method']}")
    
    # Logic B: Agar IP Risk Score 80 se zyada hai (Custom Rule)
    elif risk_score > 80:
        print(f"⚠️ WARNING: High Risk IP ({risk_score}) | ID: {tx['transaction_id']} | City: {tx['shipping_city']}")

    # Logic C: Agar transaction 5000 AED se zyada hai (High Value)
    elif amount > 5000:
        print(f"💰 NOTICE: High Value Transaction | ID: {tx['transaction_id']} | Amount: {amount} AED")
    
    else:
        print(f"✅ Clear: {tx['transaction_id']} - {amount} AED")