import customtkinter as ctk
from kafka import KafkaConsumer
import json
import threading
import pandas as pd

data = pd.read_csv("uae_ecom_fraud_100k.csv")
print(data.head())

from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

ctk.set_appearance_mode("dark")

class FraudDashboard(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("UAE E-commerce Fraud Monitoring System")
        self.geometry("1200x800")

        # Stats Variables
        self.total_count = 0
        self.fraud_count = 0
        self.safe_count = 0

        # UI Layout
        self.grid_columnconfigure(0, weight=1) # Left Panel
        self.grid_columnconfigure(1, weight=2) # Right Panel
        self.grid_rowconfigure(1, weight=1)

        # Header
        self.header = ctk.CTkLabel(self, text="🛡️ REAL-TIME FRAUD DETECTION UNIT", font=("Arial", 28, "bold"), text_color="#3b8ed0")
        self.header.grid(row=0, column=0, columnspan=2, pady=20)

        # --- LEFT PANEL (Stats & Graph) ---
        self.left_panel = ctk.CTkFrame(self, fg_color="#212121")
        self.left_panel.grid(row=1, column=0, padx=20, pady=10, sticky="nsew")

        self.lbl_total = ctk.CTkLabel(self.left_panel, text="Total: 0", font=("Arial", 22, "bold"))
        self.lbl_total.pack(pady=15)

        # Graph
        self.fig = Figure(figsize=(4, 4), facecolor='#212121')
        self.ax = self.fig.add_subplot(111)
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.left_panel)
        self.canvas.get_tk_widget().pack(pady=10)

        # Stats Labels (Neechy walay numbers)
        self.lbl_safe_stat = ctk.CTkLabel(self.left_panel, text="✅ Safe: 0", font=("Arial", 20), text_color="#2ecc71")
        self.lbl_safe_stat.pack(pady=5)

        self.lbl_fraud_stat = ctk.CTkLabel(self.left_panel, text="🚨 Fraud: 0", font=("Arial", 20), text_color="#e74c3c")
        self.lbl_fraud_stat.pack(pady=5)

        # --- RIGHT PANEL (Live Feed) ---
        self.feed_frame = ctk.CTkTextbox(self, font=("Consolas", 12), fg_color="#121212", border_color="#3b8ed0", border_width=1)
        self.feed_frame.grid(row=1, column=1, padx=20, pady=10, sticky="nsew")

        # Kafka Thread
        threading.Thread(target=self.consume_kafka, daemon=True).start()

    def update_ui(self, status):
        # Update Labels
        self.lbl_total.configure(text=f"Total Transactions: {self.total_count}")
        self.lbl_safe_stat.configure(text=f"✅ Safe Transactions: {self.safe_count}")
        self.lbl_fraud_stat.configure(text=f"🚨 Fraud Detected: {self.fraud_count}")
        
        # Update Log
        self.feed_frame.insert("end", status)
        self.feed_frame.see("end")

        # Update Graph (Har 5 trans par)
        if self.total_count % 5 == 0:
            self.ax.clear()
            labels = ['Safe', 'Fraud']
            sizes = [self.safe_count, self.fraud_count]
            if sum(sizes) > 0:
                self.ax.pie(sizes, labels=labels, autopct='%1.1f%%', colors=['#2ecc71', '#e74c3c'], textprops={'color':"w"})
            self.ax.set_title("Detection Ratio", color="white", pad=20)
            self.canvas.draw()

    def consume_kafka(self):
        consumer = KafkaConsumer(
            'fraud-topic',
            bootstrap_servers=['127.0.0.1:9092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        for message in consumer:
            tx = message.value
            self.total_count += 1
            if tx.get('is_fraud', 0) == 1:
                self.fraud_count += 1
                status = f"🚨 FRAUD: {tx['transaction_id']} | {tx['amount_aed']} AED\n"
            else:
                self.safe_count += 1
                status = f"✅ CLEAR: {tx['transaction_id']} | {tx['amount_aed']} AED\n"
            
            self.after(10, self.update_ui, status)

if __name__ == "__main__":
    app = FraudDashboard()
    app.mainloop()