import pandas as pd
import json
import numpy as np
from itertools import product
from tqdm import tqdm
from sklearn.metrics import confusion_matrix
from collections import defaultdict
import csv
import sys

def get_acc_onscore(rel_info_file, predict_file_path, output_file_path, test_file, explain_num):

    data_dict = {}

    with open(rel_info_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    print(len(lines))

    for line in lines:
        if len(line.strip().split("\t")) != 19:
            print(line)
            continue

        index, ori_query, query, query_tag, query_intent, query_may_shop, query_may_shop_name, query_may_shop_tag, query_may_item, shop_id, shop_name, nor_shop_name, category_l3_tag_local, category_l3_tag_local_idx2, manual_tag1, manual_tag2, manual_tag3, item_name_list, item_tag_list = line.strip().split("\t")

        if (ori_query, shop_id) not in data_dict:
            data_dict[(ori_query, shop_id)] = {}
        data_dict[(ori_query, shop_id)]['index'] = index
        data_dict[(ori_query, shop_id)]['normal_query'] = query
        data_dict[(ori_query, shop_id)]['query_tag'] = query_tag
        data_dict[(ori_query, shop_id)]['query_intent'] = query_intent
        data_dict[(ori_query, shop_id)]['query_may_shop'] = query_may_shop
        data_dict[(ori_query, shop_id)]['query_may_shop_name'] = query_may_shop_name
        data_dict[(ori_query, shop_id)]['query_may_shop_tag'] = query_may_shop_tag
        data_dict[(ori_query, shop_id)]['query_may_item'] = query_may_item
        data_dict[(ori_query, shop_id)]['shop_name'] = shop_name
        data_dict[(ori_query, shop_id)]['nor_shop_name'] = nor_shop_name
        data_dict[(ori_query, shop_id)]['category_l3_tag_local'] = category_l3_tag_local
        data_dict[(ori_query, shop_id)]['category_l3_tag_local_idx2'] = category_l3_tag_local_idx2
        data_dict[(ori_query, shop_id)]['manual_tag1'] = manual_tag1
        data_dict[(ori_query, shop_id)]['manual_tag2'] = manual_tag2
        data_dict[(ori_query, shop_id)]['manual_tag3'] = manual_tag3
        data_dict[(ori_query, shop_id)]['item_name_list'] = item_name_list
        data_dict[(ori_query, shop_id)]['item_tag_list'] = item_tag_list
        data_dict[(ori_query, shop_id)]['knowledge'] = f"query:{ori_query}, shop_name:{shop_name}"

    # 创建数据列表来存储所有行
    rows = []

    res = []
    invalid = 0
    unlabel = 0
    with open(predict_file_path, "r", encoding="utf-8") as f:

        for line in f:
            data = json.loads(line.strip())  # 解析单行JSON
            # if data['valid'] != 1:
            #     invalid += 1
            #     continue
            ori_query = data['ori_query']
            shop_id = data['shop_id']
            if (ori_query, shop_id) not in data_dict:
                continue
            index = int(data['index'])
            data_dict[(ori_query, shop_id)]['query_intent_type'] = data['query_intent_type']
            if data['valid'] == 1:
                if index < explain_num:
                    data_dict[(ori_query, shop_id)]['pre_Query_Type'] = data['prediction']['Query_Type']
                    data_dict[(ori_query, shop_id)]['Score'] = int(float(data['prediction']['Score']))
                    data_dict[(ori_query, shop_id)]['Explanation'] = data['prediction']['Explanation']
                else:
                    data_dict[(ori_query, shop_id)]['pre_Query_Type'] = "-"
                    data_dict[(ori_query, shop_id)]['Score'] = int(float(data['prediction']))
                    data_dict[(ori_query, shop_id)]['Explanation'] = "-"
            else:
                data_dict[(ori_query, shop_id)]['pre_Query_Type'] = "erro"
                data_dict[(ori_query, shop_id)]['Score'] = -1
                data_dict[(ori_query, shop_id)]['Explanation'] = data['prediction']

    with open(test_file, 'r', encoding='utf-8') as infile:

        for line in infile:
            parts = line.strip().split('\t')

            if len(parts) == 2:
                ori_query = parts[0]
                shop_id = parts[1]

                if (ori_query, shop_id) not in data_dict:
                    continue

                index = data_dict[(ori_query, shop_id)]['index']
                normal_query = data_dict[(ori_query, shop_id)]['normal_query']
                query_tag = data_dict[(ori_query, shop_id)]['query_tag']
                query_intent = data_dict[(ori_query, shop_id)]['query_intent']
                query_may_shop = data_dict[(ori_query, shop_id)]['query_may_shop']
                query_may_shop_name = data_dict[(ori_query, shop_id)]['query_may_shop_name']
                query_may_shop_tag = data_dict[(ori_query, shop_id)]['query_may_shop_tag']
                query_may_item = data_dict[(ori_query, shop_id)]['query_may_item']
                shop_name = data_dict[(ori_query, shop_id)]['shop_name']
                category_l3_tag_local = data_dict[(ori_query, shop_id)]['category_l3_tag_local']
                category_l3_tag_local_idx2 = data_dict[(ori_query, shop_id)]['category_l3_tag_local_idx2']
                manual_tag1 = data_dict[(ori_query, shop_id)]['manual_tag1']
                manual_tag2 = data_dict[(ori_query, shop_id)]['manual_tag2']
                manual_tag3 = data_dict[(ori_query, shop_id)]['manual_tag3']
                item_name_list = data_dict[(ori_query, shop_id)]['item_name_list']
                item_tag_list = data_dict[(ori_query, shop_id)]['item_tag_list']
                knowledge = data_dict[(ori_query, shop_id)]['knowledge']

                pre_Query_Type = data_dict[(ori_query, shop_id)].get('pre_Query_Type', '-')
                query_intent_type = data_dict[(ori_query, shop_id)].get('query_intent_type', '-')
                Score = data_dict[(ori_query, shop_id)].get('Score', -1)
                Explanation = data_dict[(ori_query, shop_id)].get('Explanation', '-')

                # 将行数据添加到列表中
                rows.append([
                    index, ori_query, normal_query, query_intent, query_intent_type, pre_Query_Type, query_tag, query_may_shop,
                    query_may_shop_name, query_may_shop_tag, query_may_item,
                    shop_id, shop_name, category_l3_tag_local, category_l3_tag_local_idx2,
                    item_name_list, item_tag_list, Score, Explanation
                ])

    # 创建DataFrame
    df = pd.DataFrame(rows, columns=[
        'index', 'ori_query', 'normal_query', 'query_intent', 'query_intent_type', 'pre_Query_Type', 'query_tag', 'query_may_shop',
        'query_may_shop_name', 'query_may_shop_tag', 'query_may_item',
        'shop_id', 'shop_name', 'category_l3_tag_local', 'category_l3_tag_local_idx2',
        'item_name_list', 'item_tag_list', 'Score', 'Explanation'
    ])

    # 保存为xlsx文件
    df.to_excel(output_file_path, index=False, engine='openpyxl')
    print(f"结果已保存到: {output_file_path}")


if __name__ == "__main__":

    rel_info_file = sys.argv[1]
    predict_file_path = sys.argv[2]
    output_file_path = sys.argv[3]
    test_file = sys.argv[4]
    explain_num = int(sys.argv[5])
    get_acc_onscore(rel_info_file, predict_file_path, output_file_path, test_file, explain_num)
