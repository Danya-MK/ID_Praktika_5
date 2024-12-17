import json
import pickle
from pymongo import MongoClient
from bson import ObjectId


def get_database():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["test_db"]
    return db["test_collection"]


def save_to_json(data, filename):
    def transform_object_id(obj):
        if isinstance(obj, dict):
            return {k: transform_object_id(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [transform_object_id(item) for item in obj]
        elif isinstance(obj, ObjectId):
            return str(obj)
        return obj

    transformed_data = transform_object_id(data)
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(transformed_data, file, ensure_ascii=False, indent=4)


def parse_pkl_file(file_path):
    with open(file_path, "rb") as file:
        data = pickle.load(file)
    return data


def insert_data_to_mongo(collection, data):
    collection.insert_many(data)


def execute_queries(collection):
    salaries = collection.aggregate(
        [
            {
                "$group": {
                    "_id": None,
                    "min_salary": {"$min": "$salary"},
                    "avg_salary": {"$avg": "$salary"},
                    "max_salary": {"$max": "$salary"},
                }
            }
        ]
    )
    save_to_json(list(salaries), "query1_salaries.json")

    professions_count = collection.aggregate(
        [{"$group": {"_id": "$job", "count": {"$sum": 1}}}]
    )
    save_to_json(list(professions_count), "query2_professions_count.json")

    salaries_by_city = collection.aggregate(
        [
            {
                "$group": {
                    "_id": "$city",
                    "min_salary": {"$min": "$salary"},
                    "avg_salary": {"$avg": "$salary"},
                    "max_salary": {"$max": "$salary"},
                }
            }
        ]
    )
    save_to_json(list(salaries_by_city), "query3_salaries_by_city.json")

    salaries_by_job = collection.aggregate(
        [
            {
                "$group": {
                    "_id": "$job",
                    "min_salary": {"$min": "$salary"},
                    "avg_salary": {"$avg": "$salary"},
                    "max_salary": {"$max": "$salary"},
                }
            }
        ]
    )
    save_to_json(list(salaries_by_job), "query4_salaries_by_job.json")

    age_by_city = collection.aggregate(
        [
            {
                "$group": {
                    "_id": "$city",
                    "min_age": {"$min": "$age"},
                    "avg_age": {"$avg": "$age"},
                    "max_age": {"$max": "$age"},
                }
            }
        ]
    )
    save_to_json(list(age_by_city), "query5_age_by_city.json")

    age_by_job = collection.aggregate(
        [
            {
                "$group": {
                    "_id": "$job",
                    "min_age": {"$min": "$age"},
                    "avg_age": {"$avg": "$age"},
                    "max_age": {"$max": "$age"},
                }
            }
        ]
    )
    save_to_json(list(age_by_job), "query6_age_by_job.json")

    max_salary_min_age = collection.aggregate(
        [{"$sort": {"age": 1, "salary": -1}}, {"$limit": 1}]
    )
    save_to_json(list(max_salary_min_age), "query7_max_salary_min_age.json")

    min_salary_max_age = collection.aggregate(
        [{"$sort": {"age": -1, "salary": 1}}, {"$limit": 1}]
    )
    save_to_json(list(min_salary_max_age), "query8_min_salary_max_age.json")

    age_city_salary = collection.aggregate(
        [
            {"$match": {"salary": {"$gt": 50000}}},
            {
                "$group": {
                    "_id": "$city",
                    "min_age": {"$min": "$age"},
                    "avg_age": {"$avg": "$age"},
                    "max_age": {"$max": "$age"},
                }
            },
            {"$sort": {"avg_age": -1}},
        ]
    )
    save_to_json(list(age_city_salary), "query9_age_city_salary.json")

    salary_by_conditions = collection.aggregate(
        [
            {
                "$match": {
                    "$or": [
                        {"age": {"$gt": 18, "$lt": 25}},
                        {"age": {"$gt": 50, "$lt": 65}},
                    ]
                }
            },
            {
                "$group": {
                    "_id": {"city": "$city", "job": "$job"},
                    "min_salary": {"$min": "$salary"},
                    "avg_salary": {"$avg": "$salary"},
                    "max_salary": {"$max": "$salary"},
                }
            },
        ]
    )
    save_to_json(list(salary_by_conditions), "query10_salary_conditions.json")

    custom_query = collection.aggregate(
        [
            {"$match": {"salary": {"$gt": 100000}}},
            {"$group": {"_id": "$job", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]
    )
    save_to_json(list(custom_query), "query11_custom_query.json")


if __name__ == "__main__":
    collection = get_database()

    pkl_data = parse_pkl_file("task_2_item.pkl")
    insert_data_to_mongo(collection, pkl_data)

    execute_queries(collection)
