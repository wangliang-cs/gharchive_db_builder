import datetime
import gzip
import json
import os
import queue
import time

import numpy as np
from pySmartDL import SmartDL

import config
from dateutil import parser


def log_unable_to_parse(line):
    with open(config.get_config("unable_to_parse_log_path"), "a", encoding="utf-8") as fd:
        fd.write(line.strip())
        fd.write('\n')


import hashlib


def generate_sha_hash(input_str, sha_version="sha256"):
    """
    生成字符串的SHA哈希值

    参数：
        input_str: 输入的字符串
        sha_version: SHA版本，支持'sha1'、'sha256'、'sha512'（默认sha256）

    返回：
        十六进制格式的SHA哈希值字符串
    """
    # 检查支持的SHA版本
    supported_versions = ["sha1", "sha256", "sha512"]
    if sha_version not in supported_versions:
        raise ValueError(f"不支持的SHA版本，可选：{supported_versions}")

    # 字符串转字节流（必须指定编码，如utf-8）
    byte_data = input_str.encode("utf-8")

    # 创建哈希对象并计算哈希值
    hash_obj = hashlib.new(sha_version, byte_data)

    # 返回十六进制结果
    return hash_obj.hexdigest()


def unzip2queue(gz_file_path, msg_out_qu):
    try:
        print(f"[{datetime.datetime.now()}] 开始处理: {gz_file_path}")
        with gzip.open(f"{gz_file_path}") as ghfd:
            lines = ghfd.readlines()
        for line in lines:
            try:
                record = json.loads(line.strip())
                if record["type"] == "GistEvent":
                    # omit GistEvents
                    continue
                record_to_send = {}
                if "id" in record:
                    record_to_send["id"] = record["id"]

                if "repo" in record:
                    repo_name = record["repo"]["name"]
                elif "repository" in record:
                    repo_name = f"{record['repository']['owner']}/{record['repository']['name']}"
                elif "url" in record:
                    repo_name = f"{record['url'].replace('https://github.com/', '')}"
                else:
                    log_unable_to_parse(line)
                    continue

                if "actor_attributes" in record:
                    actor_login = record["actor_attributes"]["login"]
                elif "actor" in record:
                    actor_login = record["actor"]["login"]
                else:
                    log_unable_to_parse(line)
                    continue
                record_to_send["proj_id"] = f"github:{repo_name}"
                record_to_send["user_id"] = f"github:{actor_login}"
                record_to_send["type"] = record["type"]
                # 统一时间格式
                created_at=record["created_at"]
                created_at=parser.parse(created_at)
                record_to_send["created_at"] = created_at.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                # record_to_send["created_at"] = record["created_at"]
                if "id" not in record:
                    id_str = f'{record_to_send["proj_id"]}_{record_to_send["user_id"]}_{record_to_send["type"]}_{record_to_send["created_at"]}'
                    record_to_send["id"] = generate_sha_hash(id_str)
                if "payload" in record:
                    if "action" in record["payload"]:
                        record_to_send["action"] = record["payload"]["action"]

                    if "issue" in record["payload"]:
                        if isinstance(record["payload"]["issue"], dict):
                            if "number" in record["payload"]["issue"]:
                                record_to_send["number"] = record["payload"]["issue"]["number"]
                        else:
                            record_to_send["number"] = record["payload"]["number"]
                    elif "issue_id" in record["payload"]:
                        record_to_send["number"] = record["payload"]["issue_id"]

                    if "pull_request" in record["payload"]:
                        if isinstance(record["payload"]["pull_request"], dict):
                            if "number" in record["payload"]["pull_request"]:
                                record_to_send["number"] = record["payload"]["pull_request"]["number"]
                        else:
                            record_to_send["number"] = record["payload"]["number"]
                if "action" not in record_to_send:
                    record_to_send["action"] = ""
                if "number" not in record_to_send:
                    record_to_send["number"] = np.nan

                while msg_out_qu.qsize() > 1000000:
                    print(f"\r[{datetime.datetime.now()}] Worker waiting for queue space.", end="")
                    time.sleep(1)
                msg_out_qu.put({"type": "record", "content": {"record": record_to_send, "gz_file_path": gz_file_path}})
            except Exception as e:
                print(e)
                print(record)
        msg_out_qu.put({"type": "complete", "content": gz_file_path})
        return True
    except Exception as e:
        print(f"[{datetime.datetime.now()}] 解压失败: {gz_file_path}\n{str(e)[:100]}")
        return False


def smart_download(url, dest_path):
    try:
        if os.path.exists(dest_path):
            if not check_ok(dest_path):
                print(f"[{datetime.datetime.now()}] 发现破损文件... {dest_path}")
                os.remove(dest_path)  # 删除无效文件
            else:
                return dest_path
        print(f"[{datetime.datetime.now()}] 发现缺失文件... {dest_path}")
        obj = SmartDL(url, dest_path, threads=8, timeout=120)
        obj.start()

        if obj.isSuccessful():
            print(f"[{datetime.datetime.now()}] 下载完成: {obj.get_dest()}")
            return dest_path
        else:
            print(f"[{datetime.datetime.now()}] 下载失败: {url}\n{obj.get_errors()}")
            return None
    except Exception as e:
        print(f"[{datetime.datetime.now()}] 下载失败: {url}\n{e}")
        return None


def check_ok(file_path):
    try:
        with gzip.open(file_path, 'rb') as f:
            f.read(1)  # 读取一个字节，确保文件可以正常读取
        return True
    except:
        return False


def gz_reader(arg_dict):
    task_in_qu = arg_dict["task_queue"]
    message_out_qu = arg_dict["message_queue"]
    worker_idx = int(arg_dict["worker_idx"])
    download_root = arg_dict["download_root"]
    complated_list = arg_dict["complated_list"]
    continue_flag = True
    while continue_flag:
        try:
            file_url = task_in_qu.get_nowait()
            filename = os.path.basename(file_url)
            year = filename.split('-')[0]
            dest_dir = os.path.join(download_root, year)
            # 确保目标目录存在
            os.makedirs(dest_dir, exist_ok=True)
            file_path = os.path.join(dest_dir, filename)

            # 检查文件是否已存在且有效
            if os.path.exists(file_path) and check_ok(file_path):
                if file_path not in complated_list:
                    if unzip2queue(file_path, message_out_qu):
                        continue
                    else:
                        os.remove(file_path)
                else:
                    print(f"\rSkipped (already exists): {file_url}", end="")
                    continue

            # 下载文件（最多重试3次）
            max_attempt = 3
            for attempt in range(max_attempt):
                try:
                    if smart_download(file_url, file_path):
                        # 验证下载的文件
                        if not check_ok(file_path):
                            os.remove(file_path)  # 删除无效文件
                            raise ValueError("Downloaded file is invalid")
                        if unzip2queue(file_path, message_out_qu):
                            break
                        else:
                            os.remove(file_path)
                except Exception as download_error:
                    if attempt == 2:  # 最后一次尝试也失败
                        raise Exception(f"Failed after 3 attempts: {file_url} - Error: {str(download_error)[:100]}")
        except queue.Empty:
            # the queue is empty
            print(f"Empty {worker_idx}")
            continue_flag = False
        except Exception as e:
            print(e)
