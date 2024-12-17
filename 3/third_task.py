import json
from pymongo import MongoClient
from bson.objectid import ObjectId


def transform_object_id(obj):
    if isinstance(obj, dict):
        return {k: transform_object_id(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [transform_object_id(item) for item in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    return obj


def load_json_data(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)


def save_to_json(data, file_path):
    transformed_data = transform_object_id(data)
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(transformed_data, file, ensure_ascii=False, indent=4)


def main():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["employment_data"]
    collection = db["jobs"]

    collection.delete_many(
        {"$or": [{"salary": {"$lt": 25000}}, {"salary": {"$gt": 175000}}]}
    )

    collection.update_many({}, {"$inc": {"age": 1}})

    selected_jobs = ["Медсестра", "Программист"]
    collection.update_many({"job": {"$in": selected_jobs}}, {"$mul": {"salary": 1.05}})

    selected_cities = ["Севилья", "Тарраса"]
    collection.update_many(
        {"city": {"$in": selected_cities}}, {"$mul": {"salary": 1.07}}
    )

    complex_predicate = {
        "$and": [
            {"city": "Астана"},
            {"job": {"$in": ["Инженер", "Строитель"]}},
            {"age": {"$gte": 20, "$lte": 40}},
        ]
    }
    collection.update_many(complex_predicate, {"$mul": {"salary": 1.10}})

    delete_predicate = {"job": "Повар"}
    collection.delete_many(delete_predicate)

    final_data = list(collection.find())
    save_to_json(final_data, "result.json")


if __name__ == "__main__":
    main()
