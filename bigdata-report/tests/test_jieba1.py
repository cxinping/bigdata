# -*- coding: utf-8 -*-
import jieba.analyse as analyse
import jieba

topK = 10
content1 = '*文具*晨光 中性笔芯 G-5 0.5mm (黑色), *文具*晨光 3#纸盒装回形针 ABS91696 28mm,*文具*晨光 金品系列陶瓷球珠中性笔 AGPH1801 0.5mm'
content2 = '*文具*晨光 中性笔芯 G-5 0.5mm (黑色)'
content3 = 'AutoBot车载吸尘器无线汽车吸尘器大功率大吸力家用车用除螨便捷迷你小型充'
content4 = '*会展服务*会议费'

content_all = u'*文具*（中石化）齐心 A858-X 大容量加厚型报告夹A4抽杆式 80张 白 10个,*文具*齐心 A1156-X 超实惠网格袋 A5 配,*金属制品*手'

jieba.analyse.set_stop_words("/you_filed_algos/app/report/algorithm/stop_words.txt")
jieba.analyse.set_idf_path("/you_filed_algos/app/report/algorithm/userdict.txt")

tags = analyse.extract_tags(content_all, topK=topK)
print(tags)

# 'ns','n', 'vn', 'v'
tags = analyse.extract_tags(content_all, topK=topK, withWeight=True, allowPOS=())

for item in tags:
    # 分别为关键词和相应的权重
    print(item[0], item[1])

