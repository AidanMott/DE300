import pymongo

# Connect to MongoDB server running on localhost at the default port (27017)
client = pymongo.MongoClient("mongodb://localhost:27017/")

# List databases
print(client.list_database_names())

# Create or access a database
mydb = client["mydatabase"]

# Create or access a collection (table)
mycollection = mydb["customers"]

data = {"name": "Alice", "age": 25, "city": "New York"}
result = mycollection.insert_one(data)
print("Inserted ID:", result.inserted_id)

data_list = [
    {"name": "Bob", "age": 30, "city": "Los Angeles"},
    {"name": "Charlie", "age": 35, "city": "Chicago"}
]
result = mycollection.insert_many(data_list)
print("Inserted IDs:", result.inserted_ids)

import json

# Load JSON file
def load_json(filename):
    with open(filename, "r") as file:
        data = json.load(file)
    return data

json_data = load_json("my-jupyter-image\\data\\data.json")  # Ensure data.json is a list of dictionaries

# Insert JSON data into MongoDB
result = mycollection.insert_many(json_data)
print("Inserted IDs:", result.inserted_ids)

import pandas as pd

# Load CSV file
def load_csv(filename):
    df = pd.read_csv(filename)
    return df.to_dict(orient="records")  # Convert DataFrame to list of dictionaries

csv_data = load_csv("my-jupyter-image\\data\\data.csv")

# Insert CSV data into MongoDB
result = mycollection.insert_many(csv_data)
print("Inserted IDs:", result.inserted_ids)

doc = mycollection.find_one()
print(doc)

for doc in mycollection.find():
    print(doc)

query = {"age": {"$gt": 28}}  # Find documents where age > 28
for doc in mycollection.find(query):
    print(doc)

query = {"name": "Alice"}
new_values = {"$set": {"age": 26}}
mycollection.update_one(query, new_values)

query = {"name": "Charlie"}
mycollection.delete_one(query)
query = {"age": {"$lt": 30}}  # Delete all documents where age < 30
mycollection.delete_many(query)

mycollection.create_index("name")
print(mycollection.index_information())

client.close()