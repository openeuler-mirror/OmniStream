import os
omni_home = os.environ.get("OMNISTREAM_HOME")

flink_file = os.path.join(omni_home,"testtool/queryfold/query16_flink/query16_result_flink.txt")
omni_file = os.path.join(omni_home,"testtool/queryfold/query16_OmniStream/flink_output.txt")
task_name_file1 = os.path.join(omni_home,"testtool/queryfold/query16_flink/query16_taskname.txt")
task_name_file2 = os.path.join(omni_home,"testtool/queryfold/query16_OmniStream/q16-taskname-OmniStream.txt")

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
    print("❌q16 failed, plan not triggered")
    exit(1)

def normalize_op_line(line):
    line = line.strip().rstrip(',')
    if not line:
        return None, None

    op = line[:2]
    data = line[3:].replace('"', '')
    return op, data

def read_changelog(path):
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
    print("❌ q16 failed, can not find file")
    exit()

flink_lines = read_changelog(flink_file)
omni_lines = read_changelog(omni_file)

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
# if not os.path.exists(flink_file) or not os.path.exists(omni_file):
#     print("❌q16 failed, the txt files were not found")
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

def compare_lines(flink_lines, omni_lines):
    max_len = max(len(flink_lines), len(omni_lines))
    print(len(flink_lines))
    print(len(omni_lines))
    print(max_len)

# for i in range(max_len):
#         flink_line = flink_lines[i] if i < len(flink_lines) else "<missing>"
#         omni_line = omni_lines[i] if i < len(omni_lines) else "<missing>"
#         if flink_line != omni_line:
#             print(f"Line {i + 1} differs:")
#             print(f"  Flink: {flink_line}")
#             print(f"  Omni : {omni_line}")
compare_lines(flink_lines, omni_lines)


if flink_lines == omni_lines:
    print("✅q16 success")
else:
    print("❌q16 failed, output results are wrong")
