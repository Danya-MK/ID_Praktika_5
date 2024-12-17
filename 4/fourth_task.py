import pymongo
import pandas as pd
import json
from pymongo import UpdateOne

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["shop_d"]
collection = db["shop_c"]

file1_path = "shopping_trends_da.csv"
file2_path = "shopping.json"

file1_data = pd.read_csv(file1_path).to_dict(orient="records")

with open(file2_path, "r") as json_file:
    file2_data = json.load(json_file)

all_data = file1_data + file2_data
collection.insert_many(all_data)


def convert_objectid_to_str(data):
    if isinstance(data, list):
        return [{**item, "_id": str(item["_id"])} for item in data]
    return {**data, "_id": str(data["_id"])}


query1 = list(collection.find().sort("Purchase Amount (USD)", -1).limit(10))
query1 = convert_objectid_to_str(query1)
with open("query1.json", "w") as f:
    json.dump(query1, f, indent=4)

query2 = list(collection.find().sort("Review Rating", -1).limit(10))
query2 = convert_objectid_to_str(query2)
with open("query2.json", "w") as f:
    json.dump(query2, f, indent=4)

query3 = list(collection.find().sort("Previous Purchases", -1).limit(15))
query3 = convert_objectid_to_str(query3)
with open("query3.json", "w") as f:
    json.dump(query3, f, indent=4)

query4 = list(collection.find({"Category": "Accessories", "Size": "M"}).limit(10))
query4 = convert_objectid_to_str(query4)
with open("query4.json", "w") as f:
    json.dump(query4, f, indent=4)

query5 = collection.count_documents(
    {
        "$and": [
            {"Item Purchased": "Sweater"},
            {"$or": [{"Age": {"$gt": 35, "$lte": 65}}]},
        ]
    }
)
with open("query5.json", "w") as f:
    json.dump(query5, f, indent=4)

age_stats = collection.aggregate(
    [
        {
            "$group": {
                "_id": None,
                "min_age": {"$min": "$Age"},
                "avg_age": {"$avg": "$Age"},
                "max_age": {"$max": "$Age"},
            }
        }
    ]
)
age_stats = list(age_stats)
with open("age_stats.json", "w") as f:
    json.dump(age_stats, f, indent=4)

male_age_stats = collection.aggregate(
    [
        {"$match": {"Gender": "Male"}},
        {
            "$group": {
                "_id": None,
                "min_age": {"$min": "$Age"},
                "avg_age": {"$avg": "$Age"},
                "max_age": {"$max": "$Age"},
            }
        },
    ]
)
male_age_stats = list(male_age_stats)
with open("male_age_stats.json", "w") as f:
    json.dump(male_age_stats, f, indent=4)

female_age_stats = collection.aggregate(
    [
        {"$match": {"Gender": "Female"}},
        {
            "$group": {
                "_id": None,
                "min_age": {"$min": "$Age"},
                "avg_age": {"$avg": "$Age"},
                "max_age": {"$max": "$Age"},
            }
        },
    ]
)
female_age_stats = list(female_age_stats)
with open("female_age_stats.json", "w") as f:
    json.dump(female_age_stats, f, indent=4)

purchase_stats = collection.aggregate(
    [
        {
            "$group": {
                "_id": None,
                "min_purchases": {"$min": "$Previous Purchases"},
                "avg_purchases": {"$avg": "$Previous Purchases"},
                "max_purchases": {"$max": "$Previous Purchases"},
            }
        }
    ]
)
purchase_stats = list(purchase_stats)
with open("purchase_stats.json", "w") as f:
    json.dump(purchase_stats, f, indent=4)

collection.delete_many({"Purchase Amount (USD)": {"$gt": 50}})

collection.update_many({}, {"$inc": {"Age": 1}})

collection.update_many(
    {"Category": "Clothing"}, {"$mul": {"Purchase Amount (USD)": 1.3}}
)

collection.update_many({"Season": "Winter"}, {"$mul": {"Purchase Amount (USD)": 1.2}})

collection.delete_many({"Color": "Lavender"})

print("Всё сделано")
