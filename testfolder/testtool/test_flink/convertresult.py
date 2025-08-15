input_file = "/repo/codehub/OmniStream/testfolder/testtool/queryfold/query22_flink/query22_result_flinkcopy.txt"
output_file = "/repo/codehub/OmniStream/testfolder/testtool/queryfold/query22_flink/query22_result_flink.txt"

with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8') as outfile:
    for line in infile:
        line = line.strip()
        if not line:
            continue

        # 提取操作符，例如 | +I | 开头
        if line.startswith("|"):
            op_end = line.find("|", 2)
            op = line[1:op_end].strip()
            rest = line[op_end+1:]
        else:
            # 不符合格式的跳过
            continue

        # 清理内容
        if rest.startswith("|"):
            rest = rest[1:]
        if rest.endswith("|"):
            rest = rest[:-1]
        parts = [part.strip() for part in rest.split("|")]

        # 写入操作符 + 数据
        outfile.write(op + "," + ",".join(parts) + "\n")

print(f"Converted '{input_file}' to '{output_file}' using commas and preserved operations.")
