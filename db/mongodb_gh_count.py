from pymongo import MongoClient, ASCENDING, UpdateOne
from pymongo.errors import BulkWriteError
import time
import datetime


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
        print(f"[{datetime.datetime.now()}] 开始处理集合 '{source_collection_name}' 到 '{target_collection_name}'")
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
        
        # 使用allowDiskUse=True和batchSize参数来优化aggregate操作
        # 同时将结果转换为列表，避免长时间保持cursor打开
        result = list(source_collection.aggregate(pipeline, allowDiskUse=True, batchSize=10000))
        
        # 记录管道执行结束时间
        pipeline_end_time = time.time()
        pipeline_time_minutes = (pipeline_end_time - pipeline_start_time) / 60
        print(f"[{datetime.datetime.now()}] 集合 '{source_collection_name}' 管道执行时间: {pipeline_time_minutes:.2f} 分钟")
        print(f"[{datetime.datetime.now()}] 聚合结果总数: {len(result)} 条记录")
        
        # 记录插入操作开始时间
        insert_start_time = time.time()
        
        # 使用批量写入方式，每次处理50000条记录
        bulk_operations = []
        batch_size = 50000
        count = 0
        
        try:
            for item in result:
                # 创建更新操作
                update_operation = UpdateOne(
                    {
                        "proj_id": item["proj_id"],
                        "user_id": item["user_id"],
                        "type": item["type"]
                    },
                    {"$set": {"count": item["count"]}},
                    upsert=True
                )
                bulk_operations.append(update_operation)
                count += 1
                
                # 当批量操作达到指定大小时，执行批量写入
                if len(bulk_operations) >= batch_size:
                    target_collection.bulk_write(bulk_operations)
                    print(f"[{datetime.datetime.now()}] 已处理 {count} 条记录")
                    bulk_operations = []
            
            # 处理剩余的记录
            if bulk_operations:
                target_collection.bulk_write(bulk_operations)
                print(f"[{datetime.datetime.now()}] 已处理 {count} 条记录")
        except BulkWriteError as bwe:
            print(f"[{datetime.datetime.now()}] 批量写入错误: {bwe.details}")
        except Exception as e:
            print(f"[{datetime.datetime.now()}] 处理过程中发生错误: {str(e)}")
        
        # 记录插入操作结束时间
        insert_end_time = time.time()
        insert_time_minutes = (insert_end_time - insert_start_time) / 60
        
        # 打印执行时间，精确到分钟
        print(f"[{datetime.datetime.now()}] 集合 '{target_collection_name}' 插入操作执行时间: {insert_time_minutes:.2f} 分钟")
        