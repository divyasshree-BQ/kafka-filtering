import json
import csv
import os
from datetime import datetime
from confluent_kafka import Consumer, KafkaError

username = 'usernammmmm'
password = 'pwww'
topic = 'solana.instructions'  # Topic for instructions

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'rpk0.bitquery.io:9093,rpk1.bitquery.io:9093,rpk2.bitquery.io:9093',
    'group.id': f'{username}-token-create-group-13',
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
    "enable.auto.commit": 'false',  # use a Boolean instead of a string
}

TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
FILTER_ADDRESS = "TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM"

def process_messages(consumer, writer, csv_file):
    """Process Kafka messages and write matching instruction rows to CSV."""
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

        try:
            buffer = msg.value().decode('utf-8')
            transaction_data = json.loads(buffer)
            if not isinstance(transaction_data, list):
                transaction_data = [transaction_data]

            for transaction in transaction_data:
                transaction_details = transaction.get("Transaction")
                if not transaction_details:
                    # print("Missing 'Transaction' detail in data")
                    continue

                transaction_signature = transaction_details.get("Signature", "N/A")
                instructions = transaction.get("Instructions")
                if not instructions:
                    # print(f"No instructions in transaction, Signature: {transaction_signature}")
                    continue

                for instruction in instructions:
                    if instruction.get("Program", {}).get("Address") != TOKEN_PROGRAM_ID:
                        continue

                    accounts = instruction.get("Accounts", [])
                    if not accounts:
                        # print(f"No accounts to process for this instruction, Signature: {transaction_signature}")
                        continue

                    for account in accounts:
                        if account and account.get("Address") == FILTER_ADDRESS:
                            signer = account.get("IsSigner", False)
                            processing_time = datetime.now().isoformat()
                            writer.writerow([transaction_signature, account.get("Address"), signer, processing_time])
                            csv_file.flush()
                            print(f"Written to CSV: Transaction {transaction_signature} at {processing_time}")

        except json.JSONDecodeError:
            print(f"Error parsing JSON: {buffer}")
        except Exception as err:
            print(f'Error processing message: {err}')

def main():
    output_file = 'bq.csv'
    file_exists = os.path.exists(output_file)
    # Open CSV once and pass the writer to the processor
    with open(output_file, 'a', newline='') as csv_file:
        writer = csv.writer(csv_file)
        if not file_exists:
            writer.writerow(['Transaction Signature', 'Account Address', 'Signer', 'Processing Time'])
            csv_file.flush()

        consumer = Consumer(conf)
        consumer.subscribe([topic])

        try:
            process_messages(consumer, writer, csv_file)
        finally:
            consumer.close()

if __name__ == "__main__":
    main()
