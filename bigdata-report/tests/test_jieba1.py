# -*- coding: utf-8 -*-
import jieba
import jieba.analyse as analyse

topK = 30
content = '*文具*晨光 中性笔芯 G-5 0.5mm (黑色), *文具*晨光 3#纸盒装回形针 ABS91696 28mm,*文具*晨光 金品系列陶瓷球珠中性笔 AGPH1801 0.5mm'
content2 = '*文具*晨光 中性笔芯 G-5 0.5mm (黑色)' #+ '*文具*晨光 3#纸盒装回形针 ABS91696 28mm' + '*文具*晨光 金品系列陶瓷球珠中性笔 AGPH1801 0.5mm'
content3 = 'AutoBot车载吸尘器无线汽车吸尘器大功率大吸力家用车用除螨便捷迷你小型充'
content4 = '*会展服务*会议费'

content_all = '*会展服务*会议费'

tags = analyse.extract_tags(content_all, topK=topK)
print(tags)

tags = analyse.extract_tags(content_all, topK=topK, withWeight=True)
print(tags)











