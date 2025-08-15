import os
import re
omni_home = os.environ.get("OMNISTREAM_HOME")

flink_file = os.path.join(omni_home,"testtool/queryfold/query0_flink/query0_result_flink.txt")
omni_file = os.path.join(omni_home,"testtool/queryfold/query0_OmniStream/flink_output.txt")
task_name_file1 = os.path.join(omni_home,"testtool/queryfold/query0_flink/query0-taskname.txt")
task_name_file2 = os.path.join(omni_home,"testtool/queryfold/query0_OmniStream/q0-taskname-OmniStream.txt")
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
    print("❌q0 failed, plan not triggered")
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
    print("❌q0 failed, the txt files were not found")
    exit(1)

with open(omni_file, "r", encoding="utf-8") as f:
    input_text = f.read()

processed_lines = process_any_broken_csv(input_text)


with open(omni_file, "w", encoding="utf-8") as f:
    for line in processed_lines:
        f.write(line + "\n")


def normalize_line(line):
    line = line.strip().rstrip(',')

    if line.startswith("+I,"):
        line = line[3:]
    if line.startswith("-I,"):
        line = line[3:]
    if line.startswith("+U,"):
        line = line[3:]
    if line.startswith("-U,"):
        line = line[3:]
    if line.startswith("+D,"):
        line = line[3:]
    if line.startswith("-D,"):
        line = line[3:]

    line = line.replace('"', '')


    line = re.sub(r'(\d{2}:\d{2}:\d{2})\.000', r'\1', line)

    return line


def read_and_normalize(path):
    with open(path, "r") as f:
        return sorted(normalize_line(l) for l in f if l.strip())

flink_lines = read_and_normalize(flink_file)
omni_lines = read_and_normalize(omni_file)

def extract_core_content(line):
    if 'Source:' not in line:
        return None
    core = line.split('Source:', 1)[1]
    return core.replace('"', '').replace(' ', '').replace(',','').rstrip(',').strip()

def compare_source_lines(file1, file2):
    if not os.path.exists(file1) or not os.path.exists(file2):
        return False

    with open(file1, 'r', encoding='utf-8') as f1, open(file2, 'r', encoding='utf-8') as f2:
        lines1 = [extract_core_content(line) for line in f1 if 'Source:' in line]
        lines2 = [extract_core_content(line) for line in f2 if 'Source:' in line]


    if len(lines1) != len(lines2):
        return False

    for i, (line1, line2) in enumerate(zip(lines1, lines2)):
        if line1 != line2:
            return False

    return True



if flink_lines == omni_lines:
    print("✅q0 success")
else:
    print("❌q0 failed, output results are wrong")
