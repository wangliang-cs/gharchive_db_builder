import datetime

from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError
import time

class GHArchiveMongoDBCountUtil:
    def __init__(self, mongodb_conn_str):
        self.mongo_client = MongoClient(mongodb_conn_str)
        self.mongo_source_db = self.mongo_client['gharchive']
        self.mongo_target_db = self.mongo_client['gharchive_count']

    def safe_create_target_collection(self, collection_name):
        if collection_name not in self.mongo_target_db.list_collection_names():
            self.mongo_target_db.create_collection(collection_name)
            # 为proj_id创建索引
            self.mongo_target_db[collection_name].create_index([("proj_id", ASCENDING)])
            # 为user_id创建索引
            self.mongo_target_db[collection_name].create_index([("user_id", ASCENDING)])
            # 为proj_id+type创建复合索引
            self.mongo_target_db[collection_name].create_index([("proj_id", ASCENDING), ("type", ASCENDING)])
            # 为user_id+type创建复合索引
            self.mongo_target_db[collection_name].create_index([("user_id", ASCENDING), ("type", ASCENDING)])
            # 为proj_id+user_id创建复合索引
            self.mongo_target_db[collection_name].create_index([("proj_id", ASCENDING), ("user_id", ASCENDING)])

    def count_events(self, source_collection_name, target_collection_name):
        if source_collection_name not in self.mongo_source_db.list_collection_names():
            print(f"[{datetime.datetime.now()}] 集合 '{source_collection_name}' 不存在")
            return
        self.safe_create_target_collection(target_collection_name)
        source_collection = self.mongo_source_db[source_collection_name]
        target_collection = self.mongo_target_db[target_collection_name]
        pipeline = [
            {"$group": {
                "_id": {"proj_id": "$proj_id", "user_id": "$user_id", "type": "$type"},
                "count": {"$sum": 1}
            }},
            {"$project": {
                "_id": 0,
                "proj_id": "$_id.proj_id",
                "user_id": "$_id.user_id",
                "type": "$_id.type",
                "count": 1
            }}
        ]
        
        # 记录管道执行开始时间
        pipeline_start_time = time.time()
        result = source_collection.aggregate(pipeline)
        # 记录管道执行结束时间
        pipeline_end_time = time.time()
        pipeline_time_minutes = (pipeline_end_time - pipeline_start_time) / 60
        
        # 记录插入操作开始时间
        insert_start_time = time.time()
        for item in result:
            target_collection.update_one(
                {
                    "proj_id": item["proj_id"],
                    "user_id": item["user_id"],
                    "type": item["type"]
                },
                {"$set": {"count": item["count"]}},
                upsert=True
            )
        # 记录插入操作结束时间
        insert_end_time = time.time()
        insert_time_minutes = (insert_end_time - insert_start_time) / 60
        
        # 打印执行时间，精确到分钟
        print(f"[{datetime.datetime.now()}] 集合 '{source_collection_name}' 管道执行时间: {pipeline_time_minutes:.2f} 分钟")
        print(f"[{datetime.datetime.now()}] 集合 '{target_collection_name}' 插入操作执行时间: {insert_time_minutes:.2f} 分钟")
        