import os
import shutil
def extract_source_lines(log_path, output_path):
    with open(log_path, 'r', encoding='utf-8') as infile, open(output_path, 'w', encoding='utf-8') as outfile:
        for line in infile:
            if '"taskName" : "Source:' in line:
                outfile.write(line)
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



if __name__ == "__main__":
    log_path = find_taskexecutor_log()


    omni_home = os.environ.get("OMNISTREAM_HOME")
    output_path = os.path.join(omni_home, "testtool/queryfold/query13_OmniStream/q13-taskname-OmniStream.txt")

    extract_source_lines(log_path, output_path)
    # print(f"✅ {output_path}")

log_file = find_taskexecutor_log()

log_home=os.environ.get("QUERY_LOGS")
query13_dir = os.path.join(log_home, "query13")
if not os.path.exists(query13_dir):
    os.makedirs(query13_dir)
log_filename = os.path.basename(log_file)
destination = os.path.join(query13_dir, log_filename)
shutil.copy2(log_file, destination)

omni_home = os.environ.get("OMNISTREAM_HOME")
output_file = os.path.join(omni_home, "testtool/queryfold/query13_OmniStream/q13-taskname-OmniStream.txt")
extract_source_lines(log_file, output_file)


