# -*- coding: utf-8 -*-

import time

#from report.commons.connect_kudu import prod_execute_sql
from report.commons.connect_kudu2 import prod_execute_sql

from report.commons.db_helper import query_kudu_data
from report.commons.logging import get_logger
from report.commons.tools import list_of_groups
import jieba.analyse as analyse
import jieba

log = get_logger(__name__)

"""
select commodityname from  01_datamart_layer_007_h_cw_df.finance_official_bill group by commodityname

"""


def execute_sql():
    sql = "select distinct commodityname from 01_datamart_layer_007_h_cw_df.finance_car_bill where commodityname is not null and commodityname !=''"
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=sql)
    commodityname_str = None

    records_ls = []
    for record in records:
        record_str = str(record[0])
        records_ls.append(record_str)

    return records_ls


def get_custom_stopwords(stop_words_file):
    with open(stop_words_file, encoding='utf-8')as f:
        stopwords = f.read()
        stopwords_list = stopwords.split('\n')
        custom_stopwords_list = [i for i in stopwords_list]
        return custom_stopwords_list


from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer


def clustering(records_ls):
    # 第一步 生成词频矩阵
    vectorizer = CountVectorizer()
    X = vectorizer.fit_transform(records_ls)
    word = vectorizer.get_feature_names()
    # for n in range(len(word)):
    #     print(word[n], end=" ")
    # print('')
    # print(X.toarray())

    # 第二步 计算TF-IDF值
    transformer = TfidfTransformer()
    # print(transformer)
    tfidf = transformer.fit_transform(X)
    # print(tfidf.toarray())
    weight = tfidf.toarray()

    # 第三步 KMeans聚类
    from sklearn.cluster import KMeans
    clf = KMeans(n_clusters=3)
    s = clf.fit(weight)
    y_pred = clf.fit_predict(weight)
    print(clf)
    print(clf.cluster_centers_)  # 类簇中心
    print('*** clf.inertia_ ==> ', clf.inertia_)  # 距离:用来评估簇的个数是否合适 越小说明簇分的越好
    print(y_pred)  # 预测类标

    # 第四步 降维处理
    from sklearn.decomposition import PCA
    pca = PCA(n_components=2)  # 降低成两维绘图
    newData = pca.fit_transform(weight)
    # print(newData)
    x = [n[0] for n in newData]
    y = [n[1] for n in newData]

    # 第五步 可视化
    import numpy as np
    import matplotlib.pyplot as plt
    plt.scatter(x, y, c=y_pred, s=100, marker='s')
    plt.title("Kmeans")
    plt.xlabel("x")
    plt.ylabel("y")
    plt.show()


def get_car_bill_jiebaword():
    """
    jieba分词 精确模式
    :return:
    """
    sql = "select distinct commodityname from 01_datamart_layer_007_h_cw_df.finance_car_bill where commodityname is not null and commodityname !=''"
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=sql)
    jiebaword = []
    for record in records:
        record_str = str(record[0])
        # 默认精确模式
        seg_list = jieba.cut(record_str, cut_all=False)
        word = "/".join(seg_list)
        # print(word)
        jiebaword.append(word)

    return jiebaword


def clean_stopword(jiebaword, stopword):
    """
    去除停用词
    :param jiebaword:
    :param stopword:
    :return:
    """
    clean_words = []

    for words in jiebaword:
        words = words.split('/')
        for word in words:
            if word not in stopword and len(str(word).strip()) > 0:
                clean_words.append(word)
    return clean_words


import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.cluster.kmeans import KMeansClusterer
from nltk.cluster.util import cosine_distance
from collections import Counter


def get_tfidf(clean_words):
    """
    生成tf-idf矩阵文档
    :return:
    """
    transformer = TfidfVectorizer()
    tfidf = transformer.fit_transform(clean_words)
    # 转为数组形式
    tfidf_arr = tfidf.toarray()
    return tfidf_arr


def get_cluster(tfidf_arr, k):
    """
    K-means聚类
    :param tfidf_arr:
    :param k:
    :return:
    """
    kmeans = KMeansClusterer(num_means=k, distance=cosine_distance, avoid_empty_clusters=True)  # 分成k类，使用余弦相似分析
    kmeans.cluster(tfidf_arr)

    # 获取分类
    kinds = pd.Series([kmeans.classify(i) for i in tfidf_arr])
    fw = open('/you_filed_algos/prod_kudu_data/ClusterText.txt', 'a+', encoding='utf-8')
    for i, v in kinds.items():
        fw.write(str(i) + '\t' + str(v) + '\n')
    fw.close()


def cluster_text(clean_words):
    """
    获取分类文档
    :return:
    """
    index_cluser = []
    try:
        with open('/you_filed_algos/prod_kudu_data/ClusterText.txt', "r", encoding='utf-8') as fr:
            lines = fr.readlines()
    except FileNotFoundError:
        print("no file like this")

    for line in lines:
        line = line.strip('\n')
        line = line.split('\t')
        index_cluser.append(line)

    for index, line in enumerate(clean_words):
        for i in range(28):
            if str(index) == index_cluser[i][0]:
                fw = open('/you_filed_algos/prod_kudu_data/cluster' + index_cluser[i][1] + '.txt', 'a+',
                          encoding='utf-8')
                fw.write(line)
    fw.close()


def get_title(cluster):
    """
     获取主题词
    :param cluster:
    :return:
    """
    for i in range(cluster):
        try:
            with open('/you_filed_algos/prod_kudu_data/cluster' + str(i) + '.txt', "r", encoding='utf-8') as fr:
                lines = fr.readlines()
        except FileNotFoundError:
            print("no file like this")
        all_words = []
        for line in lines:
            line = line.strip('\n')
            line = line.split('\t')
            for word in line:
                all_words.append(word)
        c = Counter()
        for x in all_words:
            if len(x) > 1 and x != '\r\n':
                c[x] += 1

        print('主题' + str(i + 1) + '\n词频统计结果：')
        # 输出词频最高的那个词，也可以输出多个高频词
        print('=================================')
        for (k, v) in c.most_common(1):
            print(k, ':', v, '\n')


def main():
    # records_ls = execute_sql()
    # clustering(records_ls)

    jiebaword = get_car_bill_jiebaword()
    print('jiebaword => ', jiebaword)

    stopword = get_custom_stopwords("/you_filed_algos/app/report/algorithm/stop_words.txt")

    # clean_words = clean_stopword(jiebaword, stopword)
    # print('clean_words => ', clean_words)
    # tfidf_arr = get_tfidf(clean_words)

    # print(tfidf_arr)
    # print(tfidf_arr.shape)

    # 定义聚类的个数
    # cluster = 5
    # # K-means聚类
    # get_cluster(tfidf_arr, cluster)
    #
    # # 获取分类文件
    # cluster_text(clean_words)
    #
    # # 统计出主题词
    # get_title(cluster)




main()
