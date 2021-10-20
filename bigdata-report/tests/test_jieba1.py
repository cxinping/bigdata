# -*- coding: utf-8 -*-
import jieba.analyse as analyse
import jieba

def test1():
    topK = 10
    content1 = '*文具*晨光 中性笔芯 G-5 0.5mm (黑色), *文具*晨光 3#纸盒装回形针 ABS91696 28mm,*文具*晨光 金品系列陶瓷球珠中性笔 AGPH1801 0.5mm'
    content2 = '*文具*晨光 中性笔芯 G-5 0.5mm (黑色)'
    content3 = 'AutoBot车载吸尘器无线汽车吸尘器大功率大吸力家用车用除螨便捷迷你小型充'
    content4 = '*会展服务*会议费'

    content_all = u'配送车小车清洗费,配送车大车清洗费'

    jieba.analyse.set_stop_words("/you_filed_algos/app/report/algorithm/stop_words.txt")
    jieba.analyse.set_idf_path("/you_filed_algos/app/report/algorithm/userdict.txt")

    tags = analyse.extract_tags(content_all, topK=topK)
    print(tags)

    # 'ns','n', 'vn', 'v'
    tags = analyse.extract_tags(content_all, topK=topK, withWeight=True, allowPOS=())

    for item in tags:
        # 分别为关键词和相应的权重
        print(item[0], item[1])

    print('*' * 100)

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer

def get_custom_stopwords(stop_words_file):
    with open(stop_words_file,encoding='utf-8')as f:
        stopwords=f.read()
        stopwords_list=stopwords.split('\n')
        custom_stopwords_list=[i for i in stopwords_list]
        return custom_stopwords_list

def test2():
    content_all = u'配送车小车清洗费,配送车大车清洗费'

    # 第一步 生成词频矩阵
    content = []
    stopwords = get_custom_stopwords("/you_filed_algos/app/report/algorithm/stop_words.txt")
    jieba.load_userdict('/you_filed_algos/app/report/algorithm/userdict.txt')
    seglist = jieba.cut(content_all, cut_all=False)
    final = []  # 存储去除停用词内容
    for seg in seglist:
        if seg not in stopwords:
            final.append(seg)
    #output = ' '.join(list(final))  # 空格拼接
    #print(output)
    #content.append(output)

    # 将文本中的词语转换为词频矩阵
    vectorizer = CountVectorizer()

    # 计算个词语出现的次数
    X = vectorizer.fit_transform(final)

    # 获取词袋中所有文本关键词
    word = vectorizer.get_feature_names()
    for n in range(len(word)):
        print(word[n], end=" ")
    print('')

    # 查看词频结果
    print(X.toarray())

    # 第二步 计算TF-IDF值
    transformer = TfidfTransformer()
    print(transformer)
    tfidf = transformer.fit_transform(X)
    print(tfidf.toarray())
    weight = tfidf.toarray()

    # 第三步 KMeans聚类
    from sklearn.cluster import KMeans
    clf = KMeans(n_clusters=3)
    s = clf.fit(weight)
    y_pred = clf.fit_predict(weight)
    print(clf)
    print(clf.cluster_centers_)  # 类簇中心
    print('* clf.inertia_ => ',clf.inertia_)  # 距离:用来评估簇的个数是否合适 越小说明簇分的越好
    print(y_pred)  # 预测类标



test2()


