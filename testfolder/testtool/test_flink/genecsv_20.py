import csv
import random
from datetime import datetime, timedelta

# 生成 1000 条数据
num_rows = 1000
start_time = datetime.now()

rows = []

# 每条间隔 0.5~2 秒，模拟数据流
current_time = start_time
for i in range(num_rows):
    bidder = random.randint(1, 20)  # 20个bidder左右，适当聚合
    bid_count = random.randint(1, 5)

    # 每条记录推进 0~2秒，模拟stream流动
    increment = random.uniform(0.5, 2.0)
    current_time += timedelta(seconds=increment)

    dateTime_str = current_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # 毫秒级

    rows.append([bidder, bid_count, dateTime_str])

# 写入CSV
csv_file_path = '/OmniStream/testfolder/testtool/csv_files/bid11_copy.csv'

with open(csv_file_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    for row in rows:
        writer.writerow(row)

print(f"Generated {num_rows} rows into {csv_file_path}")
