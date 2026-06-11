# coding: utf-8 
import sys
import os

import time
import math
from datetime import datetime, date, timedelta

import re
from itertools import chain

import random
import datetime as dt
import json

def safe_str(obj, encoding='utf-8'):
    """安全地将对象转换为字符串 - Python 3 最终版"""
    if obj is None:
        return ""
    if isinstance(obj, bytes):
        try:
            return obj.decode(encoding, 'ignore')
        except:
            return str(obj)
    if isinstance(obj, str):
        # 已经是字符串，直接返回
        return obj
    try:
        return str(obj)
    except:
        return repr(obj)

def preprocess_str_step(s):
    """预处理字符串 - Python 3 安全版本"""
    if s is None:
        return ""
    s = safe_str(s)
    if not s:
        return ""
    try:
        res = preprocess_str(s)
        return res if res else ""
    except Exception as e:
        # 如果出错，返回原始字符串的安全版本
        return safe_str(s)

def preprocess_str(s):
    """预处理字符串 - Python 3 安全版本"""
    if s is None or len(s) == 0:
        return ""
    
    # 统一转换为字符串
    s = safe_str(s)
    if not s:
        return ""
    
    try:
        # 此时 s 是 str 类型
        processed_query = replace_sum_alp(s.strip()).lower()
        processed_query = processed_query.replace('&#039;', "'")
        processed_query = processed_query.replace('&amp;', '&')
        processed_query = processed_query.replace("'s", '')
        processed_query = processed_query.replace("'", ' ')
        
        # Python 3 中直接使用 r'' 前缀，不需要 ur
        rule = re.compile(r'[^a-zA-Z\u3040-\u31ff\u4e00-\u9fd50-9&/*× áéíóúüÁÉÍÓÚÜñÑ()（）{}「」【】]', re.UNICODE)
        processed_query = rule.sub(' ', processed_query)
        processed_query = remove_extra_spance(processed_query)
        processed_query = processed_query.strip()
        
        return processed_query
    except Exception as e:
        # 如果出错，返回安全处理后的字符串
        return safe_str(s)

def replace_sum_alp(word):
    """替换重音符号和特殊字符 - Python 3 版本（不使用 u'' 前缀）"""
    if word is None:
        return ""
    
    # 确保是字符串
    if isinstance(word, bytes):
        word = word.decode('utf-8', 'ignore')
    elif not isinstance(word, str):
        word = str(word)
    
    # 替换映射表 - 使用普通字符串（Python 3 中默认就是 Unicode）
    replacements = {
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u', 'ü': 'u',
        'ñ': 'n', '¡': '', '¿': '',
        'Á': 'a', 'É': 'e', 'Í': 'i', 'Ó': 'o', 'Ú': 'u', 'Ü': 'u',
        'Ñ': 'n',
        'Â': 'a', 'Ê': 'e', 'Ô': 'o', 'Ã': 'a', 'Õ': 'o',
        'À': 'a', 'à': 'a', 'ã': 'a',
        'Ç': 'c', 'ç': 'c',
        'ô': 'o', 'ö': 'o', 'ê': 'e', 'â': 'a', 'ä': 'a',
        '’': "'", '‘': "'", '"': '"', '″': '"'
    }
    
    for src, dst in replacements.items():
        word = word.replace(src, dst)
    
    return word

def remove_extra_spance(text, sep=' '):
    """移除多余空格"""
    if not text:
        return ""
    if not isinstance(text, str):
        text = safe_str(text)
    temp = text.split()
    temp = [item for item in temp if len(item) > 0]
    return sep.join(temp)


def load_and_broadcast_maps_new(shop_info_dir):
    """加载所有映射数据并返回广播变量"""
    shop_data_path = shop_info_dir+ "/shop_info.txt"
    shop_name_dict = {}
    print(shop_data_path)
    with open(shop_data_path, "r") as f:
        for line in f:
            shopid, shop_name, norm_shop_name, manner_tag, items_name_info = line.strip("\n").split("\t")
            shop_name_dict[shopid] = (shop_name, norm_shop_name, manner_tag, items_name_info)

    shop_item_path = shop_info_dir+ "/shop_item_info.txt"
    shop_item_dict = {}
    print(shop_item_path)
    with open(shop_item_path, "r") as f:
        for line in f:
            item, shop_list_str = line.strip().split("\t")
            shop_list = shop_list_str.strip().split(",")
            shop_item_dict[item] = shop_list

    norm_shop_item_path = shop_info_dir+ "/norm_shop_item_info.txt"
    norm_shop_item_dict = {}
    print(norm_shop_item_path)
    with open(norm_shop_item_path, "r") as f:
        for line in f:
            item, shop_list_str = line.strip().split("\t")
            shop_list = shop_list_str.strip().split(",")
            norm_shop_item_dict[item] = shop_list

    # 打印映射大小以监控内存使用
    print(("shop_name_dict size: ", len(shop_name_dict)))
    print(("shop_item_dict_size: "), len(shop_item_dict))
    print(("norm_shop_item_dict_size: "), len(norm_shop_item_dict))
    
    # 2. 合并映射并广播
    merged_maps = {
        'shop_name_dict': shop_name_dict,
        'shop_item_dict': shop_item_dict,
        'norm_shop_item_dict': norm_shop_item_dict
    }
    return merged_maps
 
def get_may_shop_name(merged_maps_bc, query, norm_query):
    maps = merged_maps_bc
    shop_name_dict = maps["shop_name_dict"]
    shop_item_dict = maps["shop_item_dict"]
    norm_shop_item_dict = maps["norm_shop_item_dict"]
    query = preprocess_str_step(query)
    shop_num_dic = {}

    for shop in shop_name_dict:
        shop_name, norm_shop_name, manner_tag, items_name = shop_name_dict[shop]
        if not norm_shop_name in shop_num_dic:
            shop_num_dic[norm_shop_name] = 0
        shop_num_dic[norm_shop_name] += 1

    item_list = query.split(" ")
    shop_score_dic = {}
    for item in item_list:
        if not item in shop_item_dict:
            continue
        shop_list = shop_item_dict[item]
        for shop in shop_list:
            if not shop in shop_score_dic:
                shop_score_dic[shop] = 0
            shop_score_dic[shop] += len(item)

    norm_item_list = norm_query.split(" ")
    norm_shop_score_dic = {}
    for item in norm_item_list:
        if not item in norm_shop_item_dict:
            continue
        shop_list = norm_shop_item_dict[item]
        for shop in shop_list:
            if not shop in norm_shop_score_dic:
                norm_shop_score_dic[shop] = 0
            norm_shop_score_dic[shop] += len(item)

    final_shop_list = []
    shop_sorted_list = sorted(shop_score_dic.items(), key=lambda x: x[1], reverse=True)
    norm_shop_sorted_list = sorted(norm_shop_score_dic.items(), key=lambda x: x[1], reverse=True)
    i, j = 0, 0
    while i < len(shop_sorted_list) and j < len(norm_shop_sorted_list):
        if shop_sorted_list[i][1] > norm_shop_sorted_list[j][1]:
            shop = shop_sorted_list[i][0]
            i += 1
            if not shop in final_shop_list:
                final_shop_list.append(shop)
        else:
            shop = norm_shop_sorted_list[j][0]
            j += 1
            if not shop in final_shop_list:
                final_shop_list.append(shop)
        if len(final_shop_list) >= 5:
            break

    if len(final_shop_list) < 5 and i < len(shop_sorted_list):
        while i < len(shop_sorted_list):
            shop = shop_sorted_list[i][0]
            i += 1
            if not shop in final_shop_list:
                final_shop_list.append(shop)
            if len(final_shop_list) >= 5:
                break
    if len(final_shop_list) < 5 and j < len(norm_shop_sorted_list):
        while j < len(norm_shop_sorted_list):
            shop = norm_shop_sorted_list[j][0]
            j += 1
            if not shop in final_shop_list:
                final_shop_list.append(shop)
            if len(final_shop_list) >= 5:
                break
    res_list = []
    for shop in final_shop_list:
        if not shop in shop_name_dict:
            continue
        shop_name, norm_shop_name, manner_tag, items_name = shop_name_dict[shop]
        shop_num = 1
        items_name_list = items_name.split(",")
        random.shuffle(items_name_list)
        items_name = ",".join(items_name_list[:5])
        manner_tag_list = manner_tag.split(",")
        manner_tag = ",".join([x.strip() for x in manner_tag.split(',') if x.strip()])
        
        if norm_shop_name in shop_num_dic:
            shop_num = shop_num_dic[norm_shop_name]
        res_list.append(shop_name+ "~.~"+ str(shop_num)+ "~.~"+ manner_tag+ "~.~"+ items_name)
    return "!.!".join(res_list)

def get_manual_data(shop_info_dir, ori_data_path, dst_data_path):
    merged_maps = load_and_broadcast_maps_new(shop_info_dir)
    f_w = open(dst_data_path, "w")
    with open(ori_data_path, "r") as f:
        for line in f:
            items = line.strip().split("\t")
            if len(items) != 15:
                continue
            query = items[1]
            norm_query = items[2]
            shop_info = get_may_shop_name(merged_maps, query, norm_query)
            if shop_info == "":
                shop_info = "unknown"
            items.append(shop_info)
            write_line = "\t".join(items)+ "\n"
            f_w.write(write_line)

if __name__ == '__main__':

    shop_info_dir = sys.argv[1]
    ori_data_path = sys.argv[2]
    dst_data_path = sys.argv[3]
    
    get_manual_data(shop_info_dir, ori_data_path, dst_data_path)
    print('get data ok')

