import multiprocessing
import gzip
import os.path
import time

from utils import gharchive_receiver, gharchive_gzreader
import datetime
import config

do_inverse = False


def check_ok(file_path):
    try:
        with gzip.open(file_path, 'rb') as f:
            f.read(1)  # 读取一个字节，确保文件可以正常读取
        return True
    except:
        return False


def before_current_time(year, month, day, hour):
    # determine if the input time in UTC 0 is before the current system time
    try:
        input_time = time.strptime(f"{year}-{month:02d}-{day:02d} {hour}:00:00", "%Y-%m-%d %H:%M:%S")
    except:
        return 0
    current_time = time.gmtime()
    rounded_current_time = time.strptime(time.strftime("%Y-%m-%d %H:00:00", current_time), "%Y-%m-%d %H:%M:%S")
    if input_time >= rounded_current_time:
        return 1
    else:
        return 2


def generate_file_urls(start_year, end_year):
    file_urls = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            for day in range(1, 32):
                for hour in range(24):
                    bct_res = before_current_time(year, month, day, hour)
                    if bct_res == 1:
                        return file_urls
                    elif bct_res == 2:
                        file_url = f"http://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"
                        if do_inverse:
                            file_urls.insert(0, file_url)
                        else:
                            file_urls.append(file_url)
    return file_urls


def load_completed(complete_log_path):
    completed_list = set()
    try:
        if not os.path.exists(complete_log_path):
            return completed_list
        with open(complete_log_path, "r", encoding="utf-8") as fd:
            for line in fd:
                completed_list.add(line.strip())
    except Exception as e:
        print(e)
    return completed_list


def exec(start_year, end_year, num_process=10):
    # 设置下载的起始年份和结束年份
    start_year = start_year
    end_year = end_year
    print(f"[{datetime.datetime.now()}] 开始处理 {start_year} 到 {end_year} 年的文件")
    file_urls = generate_file_urls(start_year, end_year)
    complete_log_path = config.get_config("complete_log_path")
    completed_list = load_completed(complete_log_path)

    exec_start_time = time.time()
    # put tasks into queue and assemble the arg dicts
    task_qu = multiprocessing.Manager().Queue()
    for file_url in file_urls:
        task_qu.put(file_url)
    total_length = task_qu.qsize()
    # create and start message receiving worker
    msg_qu = multiprocessing.Manager().Queue()
    msg_rec = gharchive_receiver.GHReceiver(complete_log_path, msg_qu, total_length)
    msg_rec.start()
    # start main workers
    arg_list = []
    for i in range(num_process):
        arg_list.append(
            {"task_queue": task_qu, "message_queue": msg_qu, "worker_idx": i,
             "download_root": config.get_config("download_root"), "complated_list": completed_list})
    print(f"Starting {num_process} workers on {total_length} projects")
    with multiprocessing.Pool(num_process) as p:
        p.map(gharchive_gzreader.gz_reader, arg_list)
    msg_qu.put({"type": "terminate", "content": None})
    msg_rec.join()
    total_exec_time = (time.time() - exec_start_time) / 3600
    print(
        f"Terminated at {datetime.datetime.now()}, total time cost {total_exec_time:.2f} hours.")


if __name__ == "__main__":
    exec(2012, 2014, 20)
    # while True:
    #     year = datetime.datetime.now().year
    #     exec(2011, int(year), 20)
    #     time.sleep(30 * 60)  # 30min
