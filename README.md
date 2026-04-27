# Event-Streaming-pipeline
#  Real-Time UAE E-commerce Fraud Detection
A high-performance data streaming pipeline built with **Apache Kafka**, **Python**, and **CustomTkinter** to detect fraudulent transactions in real-time.
## Project Overview
This project simulates a real-time e-commerce environment in the UAE. It streams transaction data (IDs, Amounts, Cities like Dubai, Abu Dhabi, Sharjah) through Kafka, analyzes them for fraud patterns, and visualizes the results on a professional live dashboard.

### Key Features
- **Real-Time Streaming:** Uses Apache Kafka to handle high-velocity transaction data.
- **Fraud Analytics:** Backend processor that flags high-risk IP scores and suspicious amounts.
- **Interactive Dashboard:** A dark-themed GUI featuring live counters and a dynamic Pie Chart showing the Fraud-to-Safe ratio.
- **Scalable Architecture:** Separate Producer, Consumer, and Dashboard components.

## System Architecture
1. **Producer:** Reads the UAE E-commerce dataset and sends records to the Kafka topic.
2. **Kafka Broker:** Acts as the central nervous system, storing and buffering the data stream.
3. **Processor:** Analyzes the stream for specific fraud flags (`is_fraud`, `ip_risk_score`).
4. **Dashboard:** Provides a visual representation of the live stream and detection statistics.

##  Tech Stack
- **Streaming:** Apache Kafka (KRaft Mode)
- **Language:** Python 3.x
- **GUI Framework:** CustomTkinter
- **Data Handling:** Pandas
- **Visualization:** Matplotlib

##  How to Run
1. **Start Kafka:** Run the Kafka server using `server.properties`.
2. **Run Producer:** `python producer.py` to start streaming data.
3. **Run Processor:** `python processor.py` to see logic-based alerts.
4. **Run Dashboard:** `python dashboard.py` to view the live analytics.

## Dataset Insights
The system uses a dataset of 100,000 UAE-based e-commerce transactions, including:
- Transaction ID & Amount (AED)
- Payment Method (Card, PayPal, etc.)
- Shipping City (Dubai, Sharjah, Ajman, etc.)
- IP Risk Score & Fraud Flags