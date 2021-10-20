# -*- coding: utf-8 -*-

import time

from report.commons.connect_kudu import prod_execute_sql
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
    for n in range(len(word)):
        print(word[n], end=" ")
    print('')
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


def get_jiebaword():
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


def main():
    # records_ls = execute_sql()
    # clustering(records_ls)

    jiebaword = get_jiebaword()
    stopword = get_custom_stopwords("/you_filed_algos/app/report/algorithm/stop_words.txt")
    # print(stopword)
    clean_words = clean_stopword(jiebaword, stopword)
    print(clean_words)


main()

