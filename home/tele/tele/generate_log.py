import time
import random
import json
import os
import sys


def get_external_path(filename):
    # 取得執行檔所在的目錄（對於 onefile 模式，sys.argv[0] 是完整的 exe 路徑）
    exe_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    return os.path.join(exe_dir, filename)

# 取得資源檔案的路徑
CONFIG_FILE = get_external_path("config.json")
positions_file = get_external_path("positions.json")
subscribers_file = get_external_path("subscribers.json")

def load_config(file_path):
    with open(file_path, 'r',encoding='utf-8'  ) as file:
        return json.load(file)

def generate_log_entries(log_paths: list):
    # 在logs_paths中隨機選取一個路徑
    target = get_external_path(random.choice(log_paths))
    print(f"Writing to {target}")
    
    
    with open(target, "a") as target_file:
        current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        log_type = "[ERROR]" if random.random() < 0.5 else "[INFO]"
        target_file.write(f"{current_time} - {log_type} Something happened\n")

if __name__ == "__main__":
    config = load_config(CONFIG_FILE)
    log_paths = config["LOG_PATH"]
    
    while True:
        time.sleep(1)
        generate_log_entries(log_paths)
