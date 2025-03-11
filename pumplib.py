import asyncio
import websockets
import json
import csv
import os
import argparse
from datetime import datetime

# Default configuration
DEFAULT_WEBSOCKET_URI = "wss://pumpportal.fun/api/data"
DEFAULT_CSV_PATH = "mint_creation_eventsv2.csv"
# Default configuration updated to include 'tx_signature'
CSV_COLUMNS = ["event_type", "timestamp", "tx_signature", "data"]

async def handle_message(message, writer, file):
    """Handle incoming WebSocket messages and log them to the CSV file."""
    data = json.loads(message)
    event_type = "new_token"  # All messages in this script are token creation events
    tx_signature = data.get('signature', 'No Signature')  # Extracting the signature

    # Prepare the row to write to CSV
    row = {
        "event_type": event_type,
        "timestamp": datetime.now().isoformat(),
        "tx_signature": tx_signature,  # Storing the signature in its own column
        "data": json.dumps(data),  # Store the entire event data as a JSON string
    }
    # Write to CSV
    writer.writerow(row)
    file.flush()  # Ensure data is written to the file immediately

    # Output to console
    print(f"Logged {event_type} event with signature: {tx_signature}")
    print(json.dumps(data, indent=4))  # Pretty-print the event data


async def connect_and_subscribe(websocket_uri, csv_path):
    """Connect to the WebSocket server and subscribe to token creation events."""
    # Ensure the directory exists
    os.makedirs(os.path.dirname(os.path.abspath(csv_path)), exist_ok=True)
    
    while True:  # Reconnect loop
        try:
            async with websockets.connect(websocket_uri) as websocket:
                print(f"WebSocket connection established to {websocket_uri}.")

                # Subscribe to token creation events
                payload = {
                    "method": "subscribeNewToken",
                }
                await websocket.send(json.dumps(payload))
                print("Subscribed to token creation events.")

                # Open the CSV file for writing
                with open(csv_path, mode="a", newline="", encoding="utf-8") as file:
                    writer = csv.DictWriter(file, fieldnames=CSV_COLUMNS)
                    # Write header if the file is empty
                    if file.tell() == 0:
                        writer.writeheader()
                        print(f"Created new CSV file at {csv_path}")
                    else:
                        print(f"Appending to existing CSV file at {csv_path}")

                    # Listen for incoming messages
                    async for message in websocket:
                        await handle_message(message, writer, file)

        except (websockets.ConnectionClosed, ConnectionError) as e:
            print(f"WebSocket connection closed: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)  # Wait before reconnecting
        except Exception as e:
            print(f"An error occurred: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)  # Wait before reconnecting

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Token Creation Events Tracker")
    parser.add_argument(
        "--uri", 
        type=str, 
        default=DEFAULT_WEBSOCKET_URI,
        help=f"WebSocket URI (default: {DEFAULT_WEBSOCKET_URI})"
    )
    parser.add_argument(
        "--csv", 
        type=str, 
        default=DEFAULT_CSV_PATH,
        help=f"Path to the CSV file (default: {DEFAULT_CSV_PATH})"
    )
    return parser.parse_args()

async def main():
    """Main entry point for the application."""
    args = parse_arguments()
    print(f"Starting Token Creation Events Tracker")
    print(f"WebSocket URI: {args.uri}")
    print(f"CSV Path: {args.csv}")
    await connect_and_subscribe(args.uri, args.csv)

if __name__ == "__main__":
    asyncio.run(main())