# coding: utf-8 
import sys
import os

import time
import math
from datetime import datetime, date, timedelta

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from pyspark import SparkFiles
from pyspark.sql.functions import to_date, to_timestamp

import re
from itertools import chain

import random
import datetime as dt
import json
from collections import OrderedDict

partitions = 150

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

def get_final_fea(index, sid, query, normalquery, query_tag, query_intent, shop_name, nor_shop_name, category_l3_tag_local, category_l3_tag_local_idx2, manual_tag1, manual_tag2, manual_tag3, item_name_info, item_tag_info):
    
    # 特别处理已知的元组字段
    item_name_info = item_name_info.strip().replace("\t", " ").replace("\r", " ").replace("\n", " ")
    item_tag_info = item_tag_info.strip().replace("\t", " ").replace("\r", " ").replace("\n", " ")

    res = str(index)
    res_list = [query, normalquery, query_tag, query_intent, sid, shop_name, nor_shop_name, category_l3_tag_local, category_l3_tag_local_idx2, manual_tag1, manual_tag2, manual_tag3, item_name_info, item_tag_info]
    for fea in res_list:
        if fea is not None and fea.strip() != "":
            res = res + '\t' + fea.strip()
        else:
            res = res + '\t' + "unknown"
    return res

def levenshtein_distance(s1, s2):
    """计算两个字符串之间的编辑距离"""
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def find_by_edit_distance(query, string_map, max_distance=3, return_similarity=False):
    """
    使用编辑距离查找相似字符串

    Args:
        query: 查询字符串
        string_map: 字典
        max_distance: 最大允许的编辑距离
        return_similarity: 是否返回相似度分数
    """
    results = []
    query_len = len(query)

    for key, value in string_map.items():
        # 预过滤：长度差异太大的直接跳过
        if abs(len(key) - query_len) > max_distance:
            continue
        distance = levenshtein_distance(query, key)
        if distance <= max_distance:
            if return_similarity:
                # 计算相似度 (1 - 距离/最大长度)
                max_len = max(len(query), len(key))
                similarity = 1 - (distance / max_len) if max_len > 0 else 1
                results.append((key, value, distance, similarity))
            else:
                results.append((key, value, distance))
    # 按编辑距离排序
    results.sort(key=lambda x: x[2])
    return results

def get_query_may_shop(ori_query, normalquery, query_intent, shop_ori_name_map, shop_normalname_map, shop_name_map, shitu_map):
    """
    根据标准化查询和查询意图获取可能匹配的店铺
    """
    if (query_intent not in ['SI', 'FI', 'BI', 'I', 'SAI', 'C']) or query_intent is None or len(query_intent.strip()) == 0:
        # 从店铺名称映射中查找，normal_query和 shop_normalname匹配
        shop_ids = shop_normalname_map.get(normalquery, [])
        if len(shop_ids) >= 1:  # 如果多个匹配，是不是获取第一个即可，还是必须==1时进行匹配
            shop_name = shop_name_map.get(shop_ids[0], None)
            # query_intent = 'shop'
            query_intent = 'S'
            return query_intent, shop_ids[0], shop_name

        # 原始query和原始店铺名匹配
        shop_ids = shop_ori_name_map.get(replace_sum_alp(ori_query.lower()), [])
        if len(shop_ids) >= 1:
            shop_name = shop_name_map.get(shop_ids[0], None)
            # query_intent = 'shop'
            query_intent = 'S'
            return query_intent, shop_ids[0], shop_name

        # 未直接匹配到店铺名，则用原始query和原始店铺名小写且去重音符后的 编辑距离<=2的进行匹配
        edit_results = find_by_edit_distance(replace_sum_alp(ori_query.lower()), shop_ori_name_map, max_distance=2, return_similarity=True)
        if edit_results:
            shop_name, shop_ids, dist, sim = edit_results[0]
            shop_name = shop_name_map.get(shop_ids[0], None)
            query_intent = 'S'
            return query_intent, shop_ids[0], shop_name

        # 未直接匹配到店铺名，则用nomal_query和normal店铺名小写且去重音符后的 编辑距离<=2的进行匹配
        edit_results = find_by_edit_distance(replace_sum_alp(normalquery.lower()), shop_normalname_map, max_distance=2, return_similarity=True)
        if edit_results:
            shop_name, shop_ids, dist, sim = edit_results[0]
            shop_name = shop_name_map.get(shop_ids[0], None)
            query_intent = 'S'
            return query_intent, shop_ids[0], shop_name

        # 日志表query店铺匹配，原始query和店铺名匹配
        shop_id = shitu_map.get(replace_sum_alp(ori_query.lower()), None)
        if shop_id:
            shop_name = shop_name_map.get(shop_id, None)
            query_intent = 'S'
            return query_intent, shop_id, shop_name

    return query_intent, None, None

def get_query_may_item(shop_id, item_map):
    """
    根据店铺ID获取可能匹配的商品
    """
    if shop_id is None:
        return None
    return item_map.get(shop_id, None)

def get_query_may_shop_tag(shop_id, shop_tag_map):
    """
        根据店铺ID获取可能匹配的店铺主营
        """
    if shop_id is None:
        return None
    return shop_tag_map.get(shop_id, None)

def process_row(index, sid, query, normalquery, query_tag, query_intent, merged_maps_bc):
    """使用广播变量的process_row版本"""

    # 获取可能匹配的商品
    query_may_item = get_query_may_item(query_may_shop, item_map)

    query_may_shop_tag = get_query_may_shop_tag(query_may_shop, shop_tag_map)

    # 确保所有元素可哈希
    if query_may_item and isinstance(query_may_item, list):
        query_may_item = tuple(query_may_item)

    # 返回可哈希的数据结构
    # return (
    yield (
        sid,
        (
            index,
            query,
            normalquery,
            query_tag,
            query_intent,
            40%
            query_may_shop_name,
            query_may_shop_tag,
	    park.sql(query_sql).rdd.coalesce(partitions).pery_may_itey
        )
    )


def get_manual_data(source_train_hdfs, train_hdfs, fea_start_date, fea_end_date, spark_session):
    sc = spark_session.sparkContext
    spark = spark_session

    # 原始训练数据
    source_train_path = source_train_hdfs
    source_train_rdd = sc.textFile(source_train_path).map(
        lambda line : (line.strip().split('\t'))
    ).map(
        lambda (query, sid) : (query, sid)
    ).distinct().zipWithIndex().map(
        lambda ((query, sid), index): (query, (index, sid))
    )
    print("source_train_rdd_count:", source_train_rdd.count())
    print("source_train_rdd_data:", source_train_rdd.take(100))


    query_sql = '''
        select
            distinct dt,
            query,
            norm_query
        from 
            soda_international_trade_stg.gocery_query_norm_d_whole
        where
            dt >= '%s' 
            and dt <= '%s' 
            and country_code = 'BR'
            and query is not null
            and query <> ''
            and norm_query is not null
            and norm_query <> ''
    ''' % (fea_start_date, fea_end_date)
    print("query_sql:", query_sql)
    query_rdd = spark.sql(query_sql).rdd.coalesce(partitions).map(
        lambda (dt, query, normalquery): (query.lower(), (normalquery, int(dt)))
    ).reduceByKey(
        lambda a, b : a if a[1] > b[1] else b
    ).map(
        lambda (query, (normalquery, dt)) : (query, normalquery)
    ).distinct()
    print("query_rdd count: ", query_rdd.count())
    print(query_rdd.take(10))

    query_fea_sql = '''
        select
            distinct query,
            tagnamelocal,
            dt
        from
            soda_international_trade_stg.query_tag_with_geohash
        where
            dt >= '%s' 
            and dt <= '%s' 
            and groupid = 'br_grocery_v1'
            and tagtype = 'tag_algo_shop_cate'
            and tagnamelocal is not null
            and tagnamelocal <> ''
    ''' % (fea_start_date, fea_end_date)
    print("query_fea_sql:", query_fea_sql)
    query_fea_rdd = spark.sql(query_fea_sql).rdd.coalesce(partitions).map(
        lambda (normal_query, query_tag, dt): ((normal_query, dt), [query_tag.lower()])
    ).reduceByKey(
        lambda a, b : a + b
    ).map(
        lambda ((normal_query, dt), query_tag_list) : (normal_query, (int(dt), query_tag_list))
    ).reduceByKey(
        lambda a, b : a if a[0] > b[0] else b
    ).map(
        lambda (normal_query, (dt, query_tag_list)) : (normal_query, ' '.join(sorted(query_tag_list)))
    ).distinct()
    print("query_fea_rdd count: ", query_fea_rdd.count())
    print(query_fea_rdd.take(10))


    query_intent_sql = '''
        with ranked_intent as (
            select
                intent_a.query,
                b_intent.intentiontype,
                intent_a.dt as dt_a,
                b_intent.dt as dt_b,
                row_number() over (
                    partition by intent_a.query 
                    order by b_intent.dt desc, intent_a.dt desc
                ) as rn
            from
            (
                select
                    distinct query,
                    p_query,
                    dt
                from
                    soda_international_trade_stg.query_basic_intention_a
                where
                    country_code = 'BR'
                    and dt >= '%s'
            )intent_a join (
                select
                    distinct query,
                    intentiontype,
                    dt
                from
                    soda_international_trade_stg.querybasicintention
                where
                    country_code = 'BR'
                    and dt >= '%s'
            )b_intent on intent_a.p_query = b_intent.query
        )
        select query, intentiontype
        from ranked_intent
        where rn = 1
    ''' % (fea_start_date, fea_start_date)
    query_intent_rdd = spark.sql(query_intent_sql).rdd.coalesce(partitions).map(
        lambda row: (row.query.lower(), row.intentiontype)  # 先不转换意图
    ).distinct()
    print("query_intent_rdd: ", query_intent_rdd.count())
    print(query_intent_rdd.take(10))

    source_train_rdd2 = source_train_rdd.leftOuterJoin(query_rdd).map(
        lambda (query, ((index, sid), normalquery)): (normalquery if normalquery != None else query.lower(),
                                                                (query, index, sid))
    ).leftOuterJoin(query_fea_rdd).map(
        lambda (normalquery, ((query, index, sid), query_tag)): (query.lower(), (query, normalquery, index, sid, query_tag))
    ).leftOuterJoin(query_intent_rdd).map(
        lambda (query_lower, ((query, normalquery, index, sid, query_tag), query_intent)): (index, sid, query, normalquery, query_tag, query_intent)
    ).map(
        lambda (index, sid, query, normalquery, query_tag, query_intent): (sid, (index, query, normalquery, query_tag, query_intent))
    ).distinct()
    print("source_train_rdd2: ", source_train_rdd2.count())
    print(source_train_rdd2.take(10))

    shop_fea_sql = '''
        select 
            distinct t1.shopid as shop_id, 
            coalesce(t1.param['shop_name'], '') as shop_name,   -- 店铺名
            coalesce(t1.param['nor_shop_name'], '') as nor_shop_name,   --店铺名标准化
            coalesce(t2.operation_category_name, 'unknown') as category_l3_tag_local,   -- 店铺标签(主营)
            coalesce(t1.param['category_l3_tag_local_idx2'], '') as category_l3_tag_local_idx2,  -- 店铺标签(主营)
            coalesce(t1.param['manual_tag1'], '') as manual_tag1,   -- 人工标签(主营)
            coalesce(t1.param['manual_tag2'], '') as manual_tag2,   -- 人工标签(主营)
            coalesce(t1.param['manual_tag3'], '') as manual_tag3,   -- 人工标签(主营)
            coalesce(t1.param['item_name_info'], '') as item_name_info,   --菜品
            coalesce(t1.param['item_tag_info'], '') as item_tag_info      --菜品
        from 
            soda_international_trade_stg.search_rel_shop_feature t1
        left join (
            select
                shop_id,
                case
                    when operation_category = 0 then 'others'
                    when operation_category = 1 then 'market'
                    when operation_category = 2 then 'express'
                    when operation_category = 3 then 'pet'
                    when operation_category = 4 then 'alcohol'
                    when operation_category = 5 then 'baby'
                    when operation_category = 6 then 'sports and fitness'
                    when operation_category = 7 then 'technology'
                    when operation_category = 8 then 'sexual Health'
                    when operation_category = 9 then 'pharmacy'
                    when operation_category = 10 then 'stores'
                    when operation_category = 11 then 'personal care'
                    when operation_category = 12 then 'flowers'
                    when operation_category = 13 then 'office supplies'
                    when operation_category = 14 then 'warehousing'
                    when operation_category = 15 then 'butcher shop'
                    else 'others'
                end as operation_category_name
            from
                soda_international_dw_br.dwd_shop_base_d_whole
            where
                concat(year, month, day) = '%s'
                and country_code = 'BR'
                and business_type = 3
        ) t2 on t1.shopid = t2.shop_id
        where 
            t1.dt = '%s' 
            and t1.country_code = 'BR_V3' 
            and t1.param['nor_shop_name'] is not null 
            and t1.param['nor_shop_name'] <> ''
    ''' % (fea_end_date, fea_end_date)

    shop_fea_rdd = spark.sql(shop_fea_sql).rdd.coalesce(partitions).map(
        lambda (shop_id, shop_name, nor_shop_name, category_l3_tag_local, category_l3_tag_local_idx2, manual_tag1, manual_tag2, manual_tag3, item_name_info, item_tag_info): (shop_id, (shop_name, nor_shop_name, category_l3_tag_local, category_l3_tag_local_idx2, manual_tag1, manual_tag2, manual_tag3, item_name_info, item_tag_info))
    )
    print("shop_fea_rdd count: ", shop_fea_rdd.count())
    print(shop_fea_rdd.take(10))

    source_train_rdd3 = source_train_rdd2.join(shop_fea_rdd).map(
        lambda (sid, ((index, query, normalquery, query_tag, query_intent), (shop_name, nor_shop_name, category_l3_tag_local, category_l3_tag_local_idx2, manual_tag1, manual_tag2, manual_tag3, item_name_info, item_tag_info))) : get_final_fea(index, sid, query, normalquery, query_tag, query_intent, shop_name, nor_shop_name, category_l3_tag_local, category_l3_tag_local_idx2, manual_tag1, manual_tag2, manual_tag3, item_name_info, item_tag_info)
    ).distinct()
    print("source_train_rdd3: ", source_train_rdd3.count())
    print(source_train_rdd3.take(10))

    source_train_rdd3.repartition(1).saveAsTextFile(train_hdfs)
 
def load_and_broadcast_maps(fea_end_date, fea_start_date, spark_session):
    """加载所有映射数据并返回广播变量"""
    sc = spark_session.sparkContext
    spark = spark_session
    
    # 1. 加载店铺名称映射
    shop_sql = '''
        select 
            name,
            normalname,
            collect_set(shopid) as shop_ids
        from 
            soda_international_trade_stg.shopnormalname2
        where 
            dt = '%s'
            and cityid = '52090100'
        group by 
            name,
            normalname
    ''' % (fea_end_date)
    shop_df = spark.sql(shop_sql)
    shop_ori_name_map = {replace_sum_alp((row.name).lower()): row.shop_ids for row in shop_df.collect()}
    shop_normalname_map = {row.normalname: row.shop_ids for row in shop_df.collect()}
    
    # 2. 加载商品数据
    item_sql = '''
        select
            shop_id,
            collect_list(item_name) as item_names
        from
            soda_international_dwd.dwd_item_meta_d_whole
        where
            concat(year,month,day) = '%s'
            and is_sold_separately = 1
            and recall_status = 1
            and additional_type != 2
            and city_id = '52090100'
            and country_code = 'MX'
        group by 
            shop_id
    ''' % (fea_end_date)
    item_df = spark.sql(item_sql)
    item_map = {row.shop_id: row.item_names for row in item_df.collect()}
    
    # 3. 加载店铺名称
    shop_name_sql = '''
        select
            shop_id,
            shop_name
        from
            soda_international_dwd.dwd_shop_base_d_whole
        where
            country_code = 'MX'
            and city_id = '52090100'
            and concat(year,month,day) = '%s'
    ''' % (fea_end_date)
    shop_name_df = spark.sql(shop_name_sql)
    shop_name_map = {row.shop_id: row.shop_name for row in shop_name_df.collect()}
    
    # 4. 加载店铺标签
    shop_tag_sql = '''
        select
            distinct shopid as shop_id,
            -- coalesce(param['shop_name'], '') as shop_name, -- 店铺名
            coalesce(param['category_l3_tag_local'], '') as category_l3_tag_local, -- 店铺标签(主营)
            coalesce(param['category_l3_tag_local_idx2'], '') as category_l3_tag_local_idx2   -- 店铺标签(主营)
            -- coalesce(param['item_name_info'], '') as item_name_info,  --菜品
        from
            soda_international_trade_stg.search_rel_shop_feature
        where
            dt ='%s'
            and country_code='MX_V2'
            and city_id = '52090100'
            -- and param['nor_shop_name'] is not null
            -- and param['nor_shop_name'] <> ''
    ''' % (fea_end_date)
    shop_tag_df = spark.sql(shop_tag_sql)
    shop_tag_map = {row.shop_id: row.category_l3_tag_local + ", " + row.category_l3_tag_local_idx2 for row in shop_tag_df.collect()}
    
    # 5. 加载shitu数据
    shitu_sql = '''
        WITH shop_counts AS (
            SELECT 
                lower(query) as lower_query,
                s_id,
                shop_name,
                COUNT(s_id) as shop_count
            FROM
                soda_international_trade_stg.search_shitu
            WHERE
                country_code = 'MX'
                AND CONCAT(year, month, day) >= '%s'
                AND s_status = 1
                AND s_ser_status = 1
                AND s_deliveryrangestatus = 1
                AND is_cov = 1
                AND (intention_type = 1 OR intention_type = 4)    -- 品牌或者店铺意图
            GROUP BY
                lower(query), s_id, shop_name
        ),
        ranked_queries AS (
            SELECT 
                lower_query,
                s_id,
                shop_name,
                shop_count,
                ROW_NUMBER() OVER (PARTITION BY lower_query ORDER BY shop_count DESC) as rn
            FROM shop_counts
        )
        SELECT 
            lower_query as query,
            s_id,
            shop_name,
            shop_count
        FROM ranked_queries
        WHERE rn = 1
        ORDER BY shop_count DESC
    ''' % (fea_start_date)
    shitu_df = spark.sql(shitu_sql)
    shitu_map = {replace_sum_alp((row.query).lower()): row.s_id for row in shitu_df.collect()}
    
    # 打印映射大小以监控内存使用
    print(("shop_ori_name_map size: ", len(shop_ori_name_map)))
    print(("shop_normalname_map size: ", len(shop_normalname_map)))
    print(("shop_name_map size: ", len(shop_name_map)))
    print(("item_map size: ", len(item_map)))
    print(("shop_tag_map size: ", len(shop_tag_map)))
    print(("shitu_map size: ", len(shitu_map)))
    
    # 2. 合并映射并广播
    merged_maps = {
        'shop_ori_name_map': shop_ori_name_map,
        'shop_normalname_map': shop_normalname_map,
        'shop_name_map': shop_name_map,
        'shitu_map': shitu_map,
        'item_map': item_map,
        'shop_tag_map': shop_tag_map
    }

    # 3. 评估数据大小
    def safe_get_size(obj):
        """安全获取对象大小（字节）"""
        try:
            s = str(obj)
            try:
                return len(s)
            except UnicodeEncodeError:
                return len(s.encode('utf-8'))
        except:
            return 0
    
    total_size = 0
    for map_name, map_data in merged_maps.items():
        map_size = 0
        for k, v in map_data.items():
            # 键的大小
            map_size += safe_get_size(k)
            # 值的大小
            if isinstance(v, (list, tuple)):
                for item in v:
                    map_size += safe_get_size(item)
            else:
                map_size += safe_get_size(v)
        total_size += map_size
    
    total_size_mb = total_size / (1024 * 1024)
    print("Total broadcast data size: {total_size_mb:"+ str(total_size_mb)+ "} MB")
    
    # 4. 广播合并的映射
    if total_size_mb > 500:  # 如果超过500MB，警告
        print("Warning: Broadcast data is large, consider optimizing!")
    
    merged_maps_bc = sc.broadcast(merged_maps)
    return merged_maps_bc    

# 意图用原始格式
if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("rel-v2") \
        .enableHiveSupport() \
        .getOrCreate()

    source_train_hdfs = sys.argv[1]
    train_hdfs = sys.argv[2]
    use_date = sys.argv[3]

    dateTime_p = dt.datetime.strptime(use_date, '%Y%m%d').date()

    fea_end_date = str(dateTime_p.strftime('%Y%m%d'))
    fea_start_date = str((dateTime_p + dt.timedelta(days=-2)).strftime('%Y%m%d'))
    print("test_start_date: [{}], test_end_date: [{}]".format(fea_start_date, fea_end_date))

    # 执行主逻辑
    get_manual_data(source_train_hdfs, train_hdfs, fea_start_date, fea_end_date, spark)
    print('get data ok')

    spark.stop()
