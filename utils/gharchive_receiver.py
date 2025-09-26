import time
import datetime
from multiprocessing import Process

import config
from db.mongodb_gh_utilities import GHArchiveMongoDBUtil


class GHReceiver:
    def __init__(self, complete_log_path, msg_queue, total_task_count):
        self.complete_log_path = complete_log_path
        self.qu = msg_queue
        self.p = None
        self.start_time_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.start_time = time.time()
        self.total_task_count = total_task_count
        self.complete_count = 0

    def __print_estimate_time(self):
        self.complete_count += 1
        time_cost = (time.time() - self.start_time) / 3600
        time_left = ((time.time() - self.start_time) / self.complete_count) * (
                self.total_task_count - self.complete_count) / 3600
        print(
            f"Message receiver: {self.complete_count}/{self.total_task_count} "
            f"{self.complete_count / self.total_task_count * 100.:.2f}% "
            f"time cost {time_cost:.2f} hours, est. left {time_left:.2f} hours, "
            f"qsize: {self.qu.qsize()}")

    def __flush(self, msg):
        inserted_ids, write_errors = self.gh_mongo_db.flush()
        if inserted_ids >= 0:
            print(
                f"[{datetime.datetime.now()}] 完成并Flush: 插入 {inserted_ids} 条, 失败 {write_errors} 条, {msg}")

    def __record_completed(self, gz_path):
        try:
            with open(self.complete_log_path, "a", encoding="utf-8") as fd:
                fd.write(gz_path)
                fd.write("\n")
            self.__flush(gz_path)
            self.__print_estimate_time()
        except Exception as e:
            print(e)

    def __mongo_insert(self, record, gz_file_path):
        inserted_ids, write_errors = self.gh_mongo_db.insert_gh_record(record)
        if inserted_ids >= 0:
            print(
                f"[{datetime.datetime.now()}] 解压并导入进度: 插入 {inserted_ids} 条, 失败 {write_errors} 条, {gz_file_path}")

    def _worker(self):
        self.gh_mongo_db = GHArchiveMongoDBUtil(config.get_config("mongodb_conn_str"))
        continue_flag = True
        self.start_time = time.time()
        while continue_flag:
            try:
                msg = self.qu.get_nowait()
                type = msg["type"]
                content = msg["content"]
                if type == "complete":
                    self.__record_completed(content)
                elif type == "record":
                    self.__mongo_insert(content["record"], content["gz_file_path"])
                elif type == "terminate":
                    print(f"[{datetime.datetime.now()}] GHReceiver out.")
                    continue_flag = False
                else:
                    print(f"Error: unrecognized message type: {type}")
            except Exception as e:
                time.sleep(10)
                self.__flush("Receiver flush when waiting.")
                print(f"[{datetime.datetime.now()}] GHReceiver waiting. {e}")
        self.__flush("Receiver final flush.")

    def start(self):
        self.p = Process(target=self._worker, args=())
        self.p.start()

    def join(self):
        self.p.join()
