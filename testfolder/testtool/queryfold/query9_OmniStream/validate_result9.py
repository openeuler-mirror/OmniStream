import os
import re
omni_home = os.environ.get("OMNISTREAM_HOME")

flink_file = os.path.join(omni_home,"testtool/queryfold/query9_flink/query9_result_flink.txt")
omni_file = os.path.join(omni_home,"testtool/queryfold/query9_OmniStream/flink_output.txt")
task_name_file1 = os.path.join(omni_home,"testtool/queryfold/query9_flink/query9_taskname.txt")
task_name_file2 = os.path.join(omni_home,"testtool/queryfold/query9_OmniStream/q9-taskname-OmniStream.txt")

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
    print("❌q9 failed, plan not triggered")
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
if not os.path.exists(flink_file) or not os.path.exists(omni_file):
    print("❌q9 failed, the txt files were not found")
    exit()

with open(omni_file, "r", encoding="utf-8") as f:
    input_text = f.read()

processed_lines = process_any_broken_csv(input_text)


with open(omni_file, "w", encoding="utf-8") as f:
    for line in processed_lines:
        f.write(line + "\n")
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

flink_lines = read_changelog(flink_file)
omni_lines = read_changelog(omni_file)


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



# if compare_source_lines(task_name_file1,task_name_file2)==False:
#     print("❌q9 failed, native tasks do not match")

if flink_lines == omni_lines:
    print("✅q9 success")
else:
    print("❌q9 failed, output results are wrong")
    max_len = max(len(flink_lines), len(omni_lines))
    for i in range(max_len):
        f_line = flink_lines[i] if i < len(flink_lines) else "<missing>"
        o_line = omni_lines[i] if i < len(omni_lines) else "<missing>"

        if f_line != o_line:
            print(f"  ❗ Line {i+1}:")
            print(f"    flink: {f_line}")
            print(f"    omni : {o_line}")
