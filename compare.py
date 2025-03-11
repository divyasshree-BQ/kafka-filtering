import pandas as pd

# Load CSV data
pump_data = pd.read_csv('mint_creation_eventsv2.csv')
bq_data = pd.read_csv('bq.csv')
print("bq_data:", bq_data)
# Convert timestamps to datetime objects for comparison
pump_data['timestamp'] = pd.to_datetime(pump_data['timestamp'])
bq_data['Processing Time'] = pd.to_datetime(bq_data['Processing Time'])

# Assuming 'data' field in pump_data contains JSON-like string with 'signature' key
pump_data['tx_signature'] = pump_data['data'].apply(lambda x: eval(x).get('signature'))

# Merge data on transaction signature
merged_data = pd.merge(pump_data, bq_data, left_on='tx_signature', right_on='Transaction Signature', how='inner')

# Calculate which recorded first and the time difference
merged_data['time_difference'] = (merged_data['Processing Time'] - merged_data['timestamp']).dt.total_seconds()

# Check which script recorded first
merged_data['first_recorded_by'] = merged_data['time_difference'].apply(lambda x: 'pump' if x > 0 else 'bq')

# Count occurrences where each is faster
count_faster = merged_data['first_recorded_by'].value_counts()

print(merged_data[['tx_signature',  'Processing Time', 'first_recorded_by', 'time_difference']])
print("\nCounts of which script is faster:")
print(count_faster)
