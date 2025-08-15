import os
import time
import re
omni_home = os.environ.get("OMNISTREAM_HOME")

flink_file = os.path.join(omni_home,"testtool/queryfold/query19_flink/query19_result_flink.txt")
omni_file = os.path.join(omni_home,"testtool/queryfold/query19_OmniStream/flink_output.txt")


task_name_file1 = os.path.join(omni_home,"testtool/queryfold/query19_flink/query19_taskname.txt")
task_name_file2 = os.path.join(omni_home,"testtool/queryfold/query19_OmniStream/q19-taskname-OmniStream.txt")
def find_taskexecutor_log():
    flink_home = os.environ.get("FLINK_HOME")
    log_dir = os.path.join(flink_home, "log")

    candidates = []
    for file in os.listdir(log_dir):
        if ("flink-root-taskexecutor-" in file and
                "-server-" in file and
                file.endswith(".out")):
            candidates.append(os.path.join(log_dir, file))

    if not candidates:
        raise FileNotFoundError("未找到匹配的 TaskExecutor 日志文件")

    candidates.sort(key=os.path.getmtime, reverse=True)
    return candidates[0]

def check_log_for_setup_stram_task(log_file_path):
    with open(log_file_path, 'r', encoding='utf-8') as f:
        for line_number, line in enumerate(f, 1):
            if '[INFO]' in line and 'setupStramTask:' in line:
                return True
    return False


log_file = find_taskexecutor_log()
if check_log_for_setup_stram_task(log_file)==False:
    print("❌q19 failed, plan not triggered")
    exit(1)

def process_any_broken_csv(input_text):
    lines = input_text.strip().splitlines()
    result = []
    buffer = ""

    for line in lines:
        line = line.strip()
        if not line:
            continue  # 跳过空行
        if re.match(r'^(?:\+|-)[UID],', line):
            if buffer:
                result.append(buffer)
            buffer = line
        else:
            buffer += line

    if buffer:
        result.append(buffer)

    return result

with open(omni_file, "r", encoding="utf-8") as f:
    input_text = f.read()

processed_lines = process_any_broken_csv(input_text)


with open(omni_file, "w", encoding="utf-8") as f:
    for line in processed_lines:
        f.write(line + "\n")


def normalize_op_line(line):
    """返回操作符和数据（已去除引号、末尾逗号）"""
    line = line.strip().rstrip(',')
    if not line:
        return None, None

    op = line[:2]
    data = line[3:].replace('"', '')
    return op, data

def read_changelog(path):
    """处理 changelog 风格的文件，返回最终保留的数据"""
    final_rows = []
    row_set = set()

    with open(path, "r") as f:
        for line in f:
            op, data = normalize_op_line(line)
            if not data:
                continue

            if op in {"+I", "+U"}:
                final_rows.append(data)
                row_set.add(data)
            elif op == "-U" or op=="-D":
                if data in row_set:
                    row_set.remove(data)
                    final_rows.remove(data)
    return sorted(final_rows)

# 👇 替换原来的 read_and_normalize
if not os.path.exists(flink_file) or not os.path.exists(omni_file):
    print("❌ q19 failed cannot find files")
    exit()

flink_lines = read_changelog(flink_file)
omni_lines = read_changelog(omni_file)















#
#
#
#
#
#
#
#
# def normalize_line(line):
#     line = line.strip().rstrip(',')
#
#     if line.startswith("+I,"):
#         line = line[3:]
#     if line.startswith("-I,"):
#         line = line[3:]
#     if line.startswith("+U,"):
#         line = line[3:]
#     if line.startswith("-U,"):
#         line = line[3:]
#     if line.startswith("+D,"):
#         line = line[3:]
#     if line.startswith("-D,"):
#         line = line[3:]
#
#     line = line.replace('"', '')
#
#     return line
#
# def read_and_normalize(path):
#     with open(path, "r") as f:
#         return sorted(normalize_line(l) for l in f if l.strip())
#
# if not os.path.exists(omni_file):
#     print("❌q19 failed, the txt files were not found")
#     exit()
#
#
# flink_lines = read_and_normalize(flink_file)
# omni_lines = read_and_normalize(omni_file)

def extract_core_content(line):
    if 'Source:' not in line:
        return None
    core = line.split('Source:', 1)[1]
    return core.replace('"', '').replace(' ', '').replace(',', '').rstrip(',').strip()

def compare_source_lines(file1, file2):
    if not os.path.exists(file1) or not os.path.exists(file2):
        return False

    with open(file1, 'r', encoding='utf-8') as f1, open(file2, 'r', encoding='utf-8') as f2:

        lines1 = [extract_core_content(line) for line in f1 if 'Source:' in line]
        lines2 = [extract_core_content(line) for line in f2 if 'Source:' in line]

    set1 = sorted(set(filter(None, lines1)))
    set2 = sorted(set(filter(None, lines2)))


    return set1 == set2


def compare_lines_verbose(flink_lines, omni_lines):

    if len(flink_lines) != len(omni_lines):
        print(f"❌ 行数不同！flink有{len(flink_lines)}行，omni有{len(omni_lines)}行")
        return False

    success = True
    count=0
    for idx, (fline, oline) in enumerate(zip(flink_lines, omni_lines), 1):

        if fline != oline:
            count=count+1
    return success

compare_lines_verbose(flink_lines,omni_lines)

if flink_lines == omni_lines:
    print("✅q19 success")
else:
    print("❌q19 failed, output results are wrong")
