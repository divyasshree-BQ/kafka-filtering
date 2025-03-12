import pandas as pd

# Load CSV data
pump_data = pd.read_csv('mint_creation_eventsv2.csv')
bq_data = pd.read_csv('bq.csv')

# Convert timestamps to datetime objects
pump_data['timestamp'] = pd.to_datetime(pump_data['timestamp'])
bq_data['Kafka Timestamp'] = pd.to_datetime(bq_data['Kafka Timestamp'])

# Extract transaction signature from pump_data
pump_data['tx_signature'] = pump_data['data'].apply(lambda x: eval(x).get('signature'))

# Remove duplicates in bq_data by keeping the earliest Kafka Timestamp per Transaction Signature
bq_data_sorted = bq_data.sort_values(by='Kafka Timestamp')
bq_data_unique = bq_data_sorted.drop_duplicates(subset='Transaction Signature', keep='first')

# Merge data on transaction signature using unique entries from bq_data
merged_data = pd.merge(pump_data, bq_data_unique, left_on='tx_signature', right_on='Transaction Signature', how='inner')

# Calculate time difference using Kafka Timestamp
merged_data['time_difference'] = (merged_data['Kafka Timestamp'] - merged_data['timestamp']).dt.total_seconds()

# Check which script recorded first
merged_data['first_recorded_by'] = merged_data['time_difference'].apply(lambda x: 'pump' if x > 0 else 'bq')

# Count occurrences where each is faster
count_faster = merged_data['first_recorded_by'].value_counts()


print(merged_data[['tx_signature', 'Kafka Timestamp', 'first_recorded_by', 'time_difference']])
print("\nCounts of which script is faster:")
print(count_faster)


merged_data[['tx_signature','time_difference']].to_csv('time_diff.csv', index=False)
