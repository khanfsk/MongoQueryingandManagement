from pymongo import MongoClient
import sys
import json
import time
DATABASE = "MP2Norm"
COLLECTION1 = "messages"
COLLECTION2 = "senders"

def main(port):  
    port = int(port)            #not sure if needed check later
    client = MongoClient('localhost', port)  
    #if not (DATABASE in client.list_database_names()):
    db = client[DATABASE]

    collist = db.list_collection_names()
    #drop if in collections 
    if COLLECTION1 in collist:
        col = db[COLLECTION1]
        col.drop()
    #creating new collection.
    messages = db[COLLECTION1]
    print("creating database...")
    startTime = time.time()
    with open(COLLECTION1+".json",'r',encoding='utf-8') as file:
        line = file.readline()
        while line:
            i=0
            messageList = []  
            while i<5000 and line:
                try:
                    editedLine = line.strip().rstrip(",")
                    message  = json.loads(editedLine)
                    messageList.append(message)
                    line = file.readline()
                    i+=1
                except Exception as e:
                    print("line cannot be parsed to a json:", line)
                    line = file.readline()
            messages.insert_many(messageList)
            messageList.clear()
    print("time to create database:", f"{(time.time()-startTime):.2f}","seconds")
    



    #creating senders?/?????? idk
    collist = db.list_collection_names()
    #drop if in collections 
    if COLLECTION2 in collist:
        col = db[COLLECTION2]
        col.drop()
    #creating new collection.
    senders = db[COLLECTION2]
    try:
        file = open(COLLECTION2+".json","r", encoding='utf-8')
        #reading  whole into mem
        sendersDict  = json.load(file)
        senders.insert_many(sendersDict) #inserting into senders
    except Exception as e:
        print("could not read: ",e)
    finally:
        file.close()




if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python3 task1_build.py <port>")
        sys.exit(1)
        
    # Call the main function
    port = sys.argv[1]

    main(port)

