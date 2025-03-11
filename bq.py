import json
import csv
import os
import threading
import queue
import time
import uuid
from datetime import datetime
from confluent_kafka import Consumer, KafkaError

username = 'usernameeee'
password = 'pwww'
topic = 'solana.instructions'

group_id_suffix = uuid.uuid4().hex

conf = {
    'bootstrap.servers': 'rpk0.bitquery.io:9093,rpk1.bitquery.io:9093,rpk2.bitquery.io:9093',
    'group.id': f'{username}-ec2group-{group_id_suffix}',
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
    'enable.auto.commit': False,
}

TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
FILTER_ADDRESS = "TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM"

msg_queue = queue.Queue(maxsize=10000)

def consumer_thread(consumer, msg_queue):
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

def process_transaction(transaction, batch, kafka_timestamp):
    transaction_details = transaction.get("Transaction")
    if not transaction_details:
        return

    transaction_signature = transaction_details.get("Signature", "N/A")
    instructions = transaction.get("Instructions", [])

    for instruction in instructions:
        program = instruction.get("Program", {})
        if program.get("Address") != TOKEN_PROGRAM_ID:
            continue

        for account in instruction.get("Accounts", []):
            if account and account.get("Address") == FILTER_ADDRESS:
                signer = account.get("IsSigner", False)
                processing_time = datetime.now().isoformat()
                batch.append([
                    transaction_signature,
                    account.get("Address"),
                    signer,
                    processing_time,
                    kafka_timestamp
                ])

def filter_thread(msg_queue, writer, csv_file, batch_size=500):
    batch = []
    csv_lock = threading.Lock()

    while True:
        msg = msg_queue.get()
        try:
            buffer = msg.value().decode('utf-8')

            # Extract Kafka timestamp
            timestamp_type, msg_timestamp = msg.timestamp()

            # Convert Kafka timestamp to ISO format
            kafka_timestamp_iso = datetime.fromtimestamp(msg_timestamp / 1000).isoformat() if msg_timestamp else 'N/A'

            transaction_data = json.loads(buffer)

            if not isinstance(transaction_data, list):
                transaction_data = [transaction_data]

            for transaction in transaction_data:
                process_transaction(transaction, batch, kafka_timestamp_iso)

            if len(batch) >= batch_size:
                with csv_lock:
                    writer.writerows(batch)
                    csv_file.flush()
                batch.clear()

        except json.JSONDecodeError:
            print(f"Error parsing JSON: {buffer}")
        except Exception as err:
            print(f"Error processing message: {err}")
        finally:
            msg_queue.task_done()

        if len(batch) > 0 and msg_queue.empty():
            with csv_lock:
                writer.writerows(batch)
                csv_file.flush()
            batch.clear()

def main():
    output_file = 'bq.csv'
    file_exists = os.path.exists(output_file)

    with open(output_file, 'a', newline='') as csv_file:
        writer = csv.writer(csv_file)
        if not file_exists:
            writer.writerow([
                'Transaction Signature',
                'Account Address',
                'Signer',
                'Processing Time',
                'Kafka Timestamp'
            ])
            csv_file.flush()

        consumer = Consumer(conf)
        consumer.subscribe([topic])

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
