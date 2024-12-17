import pymongo
import json


def parse_file(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        content = file.read().strip()
    records = content.split("=====")
    parsed_data = []
    for record in records:
        fields = {}
        for line in record.strip().split("\n"):
            if "::" in line:
                key, value = line.split("::", 1)
                fields[key.strip()] = value.strip()
        if fields:
            parsed_data.append(fields)
    return parsed_data


def save_to_mongo(data, db_name="test_db", collection_name="test_collection"):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client[db_name]
    collection = db[collection_name]
    collection.drop()
    collection.insert_many(data)
    return collection


def execute_queries(collection):
    queries = [
        {
            "name": "query_1",
            "filter": {},
            "sort": [("salary", -1)],
            "limit": 10,
        },
        {
            "name": "query_2",
            "filter": {"age": {"$lt": 30}},
            "sort": [("salary", -1)],
            "limit": 15,
        },
        {
            "name": "query_3",
            "filter": {
                "$and": [
                    {"city": "Монсон"},
                    {"job": {"$in": ["Менеджер", "Врач", "Психолог"]}},
                ]
            },
            "sort": [("age", 1)],
            "limit": 10,
        },
        {
            "name": "query_4",
            "filter": {
                "$and": [
                    {"age": {"$gte": 30, "$lte": 50}},
                    {"year": {"$gte": 2019, "$lte": 2022}},
                    {
                        "$or": [
                            {"salary": {"$gt": 50000, "$lte": 75000}},
                            {"salary": {"$gt": 125000, "$lt": 150000}},
                        ]
                    },
                ]
            },
            "sort": None,
            "limit": 0,
        },
    ]

    for query in queries:
        cursor = collection.find(query["filter"], {"_id": 0})
        if query.get("sort"):
            cursor = cursor.sort(query["sort"])
        if query.get("limit"):
            cursor = cursor.limit(query["limit"])

        result = list(cursor)

        file_name = f"{query['name']}.json"
        with open(file_name, "w", encoding="utf-8") as file:
            json.dump(result, file, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    file_path = "task_1_item.text"
    data = parse_file(file_path)

    for record in data:
        record["salary"] = int(record["salary"])
        record["id"] = int(record["id"])
        record["year"] = int(record["year"])
        record["age"] = int(record["age"])

    collection = save_to_mongo(data)
    execute_queries(collection)
