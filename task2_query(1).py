from pymongo import MongoClient, errors
import time
import sys
flag = True

def execute_query_with_timing(db, query_func, query_description):
    """Measures and prints the execution time of a database query function."""
    try:
        start_time = time.time()
        result = query_func(db)  # Execute the query and capture the result
        end_time = time.time()
        print(f"{query_description} completed in {end_time - start_time:.4f} seconds")
        return result  # Return the result for further use
    except errors.ExecutionTimeout:
        print(f"{query_description} took longer than 2 minutes and was aborted.")
        return None  # Return None if there's a timeout
    
def q1_count_ant_messages(db):
    """Counts and returns the number of messages containing the substring 'ant' in the text field."""
    try:
        return db.messages.count_documents({"text": {"$regex": "ant"}}, maxTimeMS=120000)
    except Exception as e:
        print(f"Error in Q1: {e}")
        return None

def q2_find_top_sender(db):
    """Aggregates messages to find and return the sender with the highest number of messages."""
    try:
        top_sender = db.messages.aggregate([
            {"$group": {"_id": "$sender_info.sender_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ], maxTimeMS=120000)
        return list(top_sender)
    except Exception as e:
        print(f"Error in Q2: {e}")
        return None

def q3_count_zero_credit(db):
    """Counts and returns the number of messages where the sender's credit is zero."""
    try:
        return db.messages.count_documents({"sender_info.credit": 0}, maxTimeMS=120000)
    except Exception as e:
        print(f"Error in Q3: {e}")
        return None

def q4_double_credit(db):
    """Doubles the credit for senders whose credit is less than 100 and returns the count of modified documents."""
    try:
        update_result = db.messages.update_many(
            {"sender_info.credit": {"$lt": 100}},
            {"$mul": {"sender_info.credit": 2}}
        )
        return update_result.modified_count
    except Exception as e:
        print(f"Error in Q4: {e}")
        return None

def create_indices(db):
    """Creates ascending and text indices on specified fields in the messages collection."""
    # ASC index for sender_id within the embedded sender_info
    db.messages.create_index([("sender_info.sender_id", 1)])
    
    # Text index for the text field for full-text search
    db.messages.create_index([("text", "text")])

    # Additional ASC index for the credit within the embedded sender_info
    db.messages.create_index([("sender_info.credit", 1)])

def execute_queries(db):
    """Executes the specified query functions (Q1, Q2, Q3, and conditionally Q4) and prints their results."""
    global flag
    result_q1 = execute_query_with_timing(db, q1_count_ant_messages, "Q1")
    print(f"Q1: Number of messages containing 'ant': {result_q1}")
    
    result_q2 = execute_query_with_timing(db, q2_find_top_sender, "Q2")
    print(f"Q2: Top sender: {result_q2}")
    
    result_q3 = execute_query_with_timing(db, q3_count_zero_credit, "Q3")
    print(f"Q3: Messages from senders with 0 credit: {result_q3}")

    # Only run Q4 once
    if flag:
        execute_query_with_timing(db, q4_double_credit, "Q4")
        print(f"Q4: Doubled the credit of all senders whose credit is less than 100!")
        flag = False


def main(port):
    client = MongoClient('localhost', port)
    db = client.MP2Embd
    
    print("Executing queries before indexing:")
    execute_queries(db)

    print("\nCreating indices...")
    create_indices(db)
    
    print("\nExecuting queries after indexing:")
    execute_queries(db)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 task2_build.py <port>")
        sys.exit(1)
    port = int(sys.argv[1])
    main(port)
