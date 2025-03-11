import json
import csv
import os
import threading
import queue
import time
import uuid  # Import the uuid module
from datetime import datetime
from confluent_kafka import Consumer, KafkaError

username = 'usernameee'
password = 'pwwww'
topic = 'solana.instructions'  # Topic for instructions

# Generate a random UUID for the group ID suffix
group_id_suffix = uuid.uuid4().hex  

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'rpk0.bitquery.io:9093,rpk1.bitquery.io:9093,rpk2.bitquery.io:9093',
    'group.id': f'{username}-ec2group-{group_id_suffix}',  # Use the random UUID here
    'session.timeout.ms': 30000,
    'security.protocol': 'SASL_SSL',
    'ssl.ca.location': 'server.cer.pem',
    'ssl.key.location': 'client.key.pem',
    'ssl.certificate.location': 'client.cer.pem',
    'ssl.endpoint.identification.algorithm': 'none',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': username,
    'sasl.password': password,
    'auto.offset.reset': 'latest',
    "enable.auto.commit": False,
}

TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
FILTER_ADDRESS = "TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM"

# Create a queue for passing messages between threads
msg_queue = queue.Queue(maxsize=10000)

def consumer_thread(consumer, msg_queue):
    """Thread to consume messages from Kafka and put them in the queue."""
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error in partition: {msg.error()}")
                continue
        msg_queue.put(msg)

def filter_thread(msg_queue, writer, csv_file):
    """Thread to filter messages from the queue and write to CSV."""
    while True:
        msg = msg_queue.get()  # Blocks until a message is available
        try:
            buffer = msg.value().decode('utf-8')
            transaction_data = json.loads(buffer)
            if not isinstance(transaction_data, list):
                transaction_data = [transaction_data]

            for transaction in transaction_data:
                transaction_details = transaction.get("Transaction")
                if not transaction_details:
                    print("Missing 'Transaction' detail in data")
                    continue

                transaction_signature = transaction_details.get("Signature", "N/A")
                instructions = transaction.get("Instructions")
                if not instructions:
                    continue

                for instruction in instructions:
                    if instruction.get("Program", {}).get("Address") != TOKEN_PROGRAM_ID:
                        continue

                    accounts = instruction.get("Accounts", [])
                    if not accounts:
                        continue

                    for account in accounts:
                        if account and account.get("Address") == FILTER_ADDRESS:
                            signer = account.get("IsSigner", False)
                            processing_time = datetime.now().isoformat()
                            writer.writerow([transaction_signature, account.get("Address"), signer, processing_time])
                            csv_file.flush()

        except json.JSONDecodeError:
            print(f"Error parsing JSON: {buffer}")
        except Exception as err:
            print(f"Error processing message: {err}")
        finally:
            msg_queue.task_done()

def main():
    output_file = 'bq.csv'
    file_exists = os.path.exists(output_file)
    # Open CSV once and pass the writer to the filtering thread
    with open(output_file, 'a', newline='') as csv_file:
        writer = csv.writer(csv_file)
        if not file_exists:
            writer.writerow(['Transaction Signature', 'Account Address', 'Signer', 'Processing Time'])
            csv_file.flush()

        consumer = Consumer(conf)
        consumer.subscribe([topic])

        # Create and start threads
        t1 = threading.Thread(target=consumer_thread, args=(consumer, msg_queue), daemon=True)
        t2 = threading.Thread(target=filter_thread, args=(msg_queue, writer, csv_file), daemon=True)

        t1.start()
        t2.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Stopping threads...")
        finally:
            consumer.close()

if __name__ == "__main__":
    main()
