import datetime

from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError


class GHArchiveMongoDBUtil:

    def __init__(self, mongodb_conn_str):
        self.mongo_client = MongoClient(mongodb_conn_str)
        self.mongo_db = self.mongo_client['gharchive']
        # for year in [2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]:
        #     for month in ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]:
        #         collection_name = f"events_id_{year}_{month}"
        #         self.safe_create_collection_with_indexes(collection_name)
        self.buffer = {}

    def safe_create_collection_with_indexes(self, collection_name):
        """
        安全地创建集合和索引（先检查集合是否存在）
        """
        # 获取集合列表
        collection_names = self.mongo_db.list_collection_names()

        if collection_name not in collection_names:
            # 显式创建集合（可以添加选项如 capped, size 等）
            self.mongo_db.create_collection(collection_name)
            print(f"集合 {collection_name} 已创建")
        else:
            return
            # print(f"集合 {collection_name} 已存在")

        collection = self.mongo_db[collection_name]

        # 定义要创建的索引
        indexes_to_create = [
            {
                "keys": [("id", ASCENDING)],
                "options": {"unique": True}
            },
            {
                "keys": [("proj_id", ASCENDING)],
                "options": {"unique": False}
            },
            {
                "keys": [("user_id", ASCENDING)],
                "options": {"unique": False}
            },
            {
                "keys": [("proj_id", ASCENDING), ("type", ASCENDING)],
            },
            {
                "keys": [("user_id", ASCENDING), ("type", ASCENDING)],
            },
        ]

        # 获取现有索引
        existing_indexes = collection.index_information()

        # 创建缺失的索引
        for index_def in indexes_to_create:
            # 生成索引名称（如果未指定）
            keys = index_def["keys"]
            options = index_def.get("options", {})
            index_name = options.get("name", "_".join([f"{k[0]}_{k[1]}" for k in keys]))

            if index_name not in existing_indexes:
                try:
                    collection.create_index(keys, **options)
                except Exception as e:
                    print(e)
                    print(f"创建索引失败: {index_name}")
                print(f"已创建索引: {index_name}")
            else:
                print(f"索引已存在: {index_name}")

    def insert_gh_record(self, gh_record):
        date_str = gh_record["created_at"]
        year = date_str.split('-')[0]
        month = date_str.split('-')[1]
        if int(year) <= 2021:
            col_id = f"{year}_{month}_neo"
        else:
            col_id = f"{year}_{month}"
        return self.__buffer_flush(gh_record, col_id)

    def __buffer_flush(self, extended_gh_record, col_id: str):
        if col_id not in self.buffer:
            self.buffer[col_id] = []
        self.buffer[col_id].append(extended_gh_record)
        if len(self.buffer[col_id]) >= 50000:
            return self.__do_insert_many(col_id)
        else:
            return -1, -1

    def get_collection(self, collection_name):
        return self.mongo_db[collection_name]

    def flush(self):
        inserted_ids = 0
        write_errors = 0
        for col_id in self.buffer:
            if len(self.buffer[col_id]) > 0:
                s, e = self.__do_insert_many(col_id)
                inserted_ids += s
                write_errors += e
        return inserted_ids, write_errors

    def __do_insert_many(self, col_id: str):
        events_collection_name = f"events_id_{col_id}"
        self.safe_create_collection_with_indexes(events_collection_name)
        print(f"[{datetime.datetime.now()}] Do insert many {events_collection_name}")
        collection = self.mongo_db[events_collection_name]
        try:
            result = collection.insert_many(self.buffer[col_id], ordered=False)
            # print(f"成功插入 {len(result.inserted_ids)} 个文档")
            inserted_ids = len(result.inserted_ids)
            write_errors = []
        except BulkWriteError as e:
            # 获取成功插入的文档
            inserted_ids = e.details['nInserted']
            write_errors = e.details['writeErrors']
            #
            # print(f"成功插入 {inserted_ids} 个文档")
            # print(f"有 {len(write_errors)} 个文档插入失败")
        finally:
            self.buffer[col_id] = []
        return inserted_ids, len(write_errors)

# singleton_gh_mongo = GHArchiveMongoDBUtil(config.get_config("mongodb_conn_str"))