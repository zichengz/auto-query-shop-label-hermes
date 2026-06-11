import pandas as pd
import json
import numpy as np
from itertools import product
import csv
import sys

def get_score(file_path, explain_num):
    query_shop_dict = {}
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            data = json.loads(line.strip())  # 解析单行JSON
            ori_query = data['ori_query']
            shop_id = data['shop_id']
            index = int(data['index'])
            try:
                if index < explain_num:
                    Score = int(float(data['prediction']['Score']))
                    # pred_label = 1 if Score == 1 else 0
                else:
                    Score = int(float(data['prediction']))
                pred_label = Score
                query_shop_dict[(ori_query, shop_id)] = pred_label
            except (TypeError, KeyError) as e:
                print(f"错误: {e}")
                print("data 结构:", data)

        # print(len(query_shop_dict.keys()))
    return query_shop_dict

def txt_add_label(query_shop_dict, test_file, result_path):

    try:
        num_nolabel = 0
        num_label = 0
        with open(test_file, 'r', encoding='utf-8') as infile, \
                open(result_path, 'w', encoding='utf-8') as outfile:

            for line in infile:
                parts = line.strip().split('\t')

                if len(parts) == 2:
                    query = parts[0]
                    shop_id = parts[1]

                    if (query, shop_id) in query_shop_dict:
                        num_label += 1
                        outfile.write(query + '\t' + shop_id + '\t' + str(query_shop_dict[(query, shop_id)]) + '\n')
                    else:
                        num_nolabel += 1
                        outfile.write(query + '\t' + shop_id + '\t' + '-1' + '\n')

        print("num_nolabel", num_nolabel)
        print("num_label", num_label)
        print(f"处理完成！结果已保存到 {result_path}")

    except FileNotFoundError:
        print(f"错误：找不到文件 {test_file}")
    except Exception as e:
        print(f"处理文件时出错: {e}")


if __name__ == '__main__':

    predict_jsonl = sys.argv[1]
    test_file = sys.argv[2]
    result_path  = sys.argv[3]
    explain_num = int(sys.argv[4])
    query_shop_dict = get_score(predict_jsonl, explain_num)
    txt_add_label(query_shop_dict, test_file, result_path)
