import sys
import time
import json  # Importing the json module
from pymongo import MongoClient

def load_senders(filepath):
    """Load senders from a JSON file and return a dictionary with sender_id as keys."""
    start_time = time.time()
    with open(filepath, 'r', encoding='utf-8') as file:
        senders_list = json.load(file)  # Directly loading JSON data into a Python object
    senders_dict = {sender['sender_id'].strip(): sender for sender in senders_list} # Creates a dictionary mapping sender IDs to their information from a list of sender dictionaries
    end_time = time.time()
    print(f"Time to load and process senders: {end_time - start_time:.4f} seconds")
    return senders_dict

def batch_insert_messages(filepath, senders_dict, messages_collection, batch_size=5000):
    """Read messages from a file, embed sender info, and batch insert into MongoDB."""
    start_time = time.time()
    batch = []
    with open(filepath, 'r', encoding='utf-8') as file:
        for line_num, line in enumerate(file, 1):
            try:
                message = json.loads(line.strip().rstrip(','))  # Parse each line as JSON
                if message:
                    message = embed_sender_info(message, senders_dict)
                    batch.append(message)
                    if len(batch) == batch_size:
                        messages_collection.insert_many(batch)
                        batch.clear()
            except json.JSONDecodeError:
                print(f"Skipping invalid JSON at line {line_num}")
        if batch:  # Insert any remaining messages
            messages_collection.insert_many(batch)
    end_time = time.time()
    print(f"Time to load and insert messages: {end_time - start_time:.4f} seconds")


def embed_sender_info(message, senders_dict):
    """Add/Embed sender information into a message."""
    sender = senders_dict.get(message['sender'].strip()) # Fetches sender info by ID
    message['sender_info'] = sender if sender else {} # Assigns sender info to message if there is one else empty dict
    return message

def main(port):
    """Main function to set up MongoDB connection and process the files."""
    client = MongoClient('localhost', port)
    db = client.MP2Embd
    messages_collection = db.messages
    messages_collection.drop()  # Clear existing data

    senders_dict = load_senders('senders.json')
    batch_insert_messages('messages.json', senders_dict, messages_collection)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 task2_build.py <port>")
        sys.exit(1)
    port = int(sys.argv[1])
    main(port)
