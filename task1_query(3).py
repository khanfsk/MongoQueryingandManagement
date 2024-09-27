from pymongo import MongoClient
import sys
import time # need this for the time consideration



def query_1(db):
    # Outputs the count of messages containing 'ant' along with the execution time.
    start_time = time.time()
    count = db.messages.count_documents({"text": {"$regex": "ant"}}, maxTimeMS=120000)
    print(f"Q1. Number of messages with 'ant': {count}")
    print(f"Time taken: {time.time() - start_time:.4f} seconds")

def query_2(db):
    # Outputs the top sender based on message count along with the execution time.
    start_time = time.time()
    top_sender = db.messages.aggregate([
        {"$group": {"_id": "$sender", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ], maxTimeMS=120000)
    for sender in top_sender:
        print(f"Q2. Top sender: {sender['_id']} with {sender['count']} messages")
    print(f"Time taken: {time.time() - start_time:.4f} seconds")

def query_3(db):
    # Outputs the number of messages from senders with 0 credit along with the execution time.
    start_time = time.time()
    sender_ids_list = db.senders.find({'credit': 0}).distinct('sender_id')
    count_q3 = db.messages.count_documents({'sender': {'$in': sender_ids_list}}, maxTimeMS=120000)
    print(f"Q3. Number of messages from senders with 0 credit: {count_q3}")
    print(f"Time taken: {time.time() - start_time:.4f} seconds")


def query_4(db):
    # Outputs the count of senders whose credit was doubled because it was less than 100, along with the execution time.
    start_time = time.time()
    result = db.senders.update_many(
        {"credit": {"$lt": 100}}, 
        {"$mul": {"credit": 2}},
    )
    print(f"Q4. Updated {result.modified_count} senders' credit.")
    print(f"Time taken: {time.time() - start_time:.4f} seconds")


def main(port):
    client = MongoClient('localhost', port)
    db = client.MP2Norm  
    try:
        query_1(db)
    except Exception as e:
        print(e)
    try:
        query_2(db)
    except Exception as e:
        print(e)
    try:
        query_3(db)
    except Exception as e:
        print(e)
    try:
        query_4(db)
    except Exception as e:
        print(e)
    
    print("\nCreating indices...")
    db.messages.create_index([('sender', 1)])
    db.messages.create_index([('text', 'text')])
    db.senders.create_index([('sender_id', 1)])

    try:
        query_1(db)
    except Exception as e:
        print(e)
    try:
        query_2(db)
    except Exception as e:
        print(e)
    try:
        query_3(db)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 task1_query.py <port>")
        sys.exit(1)
    port = int(sys.argv[1])
    main(port)
