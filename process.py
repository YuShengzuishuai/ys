import concurrent.futures
import os
from request_yiyan import request_yiyan_post,prompt
from tqdm import tqdm

# 并发请求模型

def process_data(lines):
    res = []
    fail_data = []
    for line in tqdm(lines):
        data = line.strip().split("\t")
        words = data[1]
        desc = data[7]
        model_input = prompt.format(words, desc)
        model_res = request_yiyan_post(model_input, yiyan_type="4")
        data.append(model_res)
        try:
            res.append("\t".join(data))
        except:
            fail_data.append(line)
            continue
    # 覆盖原先的失败数据 重新写入
    with open("multi_process_res/fail_data_1", 'w') as f:
        for line in f:
            f.write(line)
    return res

def process_part(lines, part_index):
    # 请求模型逻辑
    result = process_data(lines)
    output_file = f"multi_process_res/output_file_{part_index}"
    # 追加写入
    with open(output_file, 'a+') as f:
        for line in result:
            f.write(line+'\n')
    print(f"Part {part_index} processed and saved to {output_file}")

def main():
    large_file = "..."
    #并发量
    num_parts = 4

    # 读取大文件
    with open(large_file, 'r') as f:
        lines = f.readlines()

    # 计算每个部分的行数
    total_lines = len(lines)

    # 若请求有失败，持续请求
    while total_lines > 0:
        # 切割并并行处理每个部分
        lines_per_part = (total_lines + num_parts - 1) // num_parts
        with concurrent.futures.ProcessPoolExecutor() as executor:
            futures = []
            for i in range(num_parts):
                start_index = i * lines_per_part
                end_index = min(start_index + lines_per_part, total_lines)
                part_lines = lines[start_index:end_index]
                futures.append(executor.submit(process_part, part_lines, i))

            # 等待所有任务完成
            for future in concurrent.futures.as_completed(futures):
                future.result()
        # 读取这一轮请求失败的数据
        with open("multi_process_res/fail_data_1", 'r') as f:
            lines = f.readlines()
            total_lines = len(lines)

    print("所有部分处理完成。")

if __name__ == "__main__":
    main()