# -*- coding: utf-8 -*-
from report.commons.tools import *


def demo1():
    """
        1，优先找最细的行政单位
        2，加两列，所属省
           出发地所在省份， 目的地所在省份

        """

    match_area1 = MatchArea()
    # data = '山东省东营市东营区开萍乡北二路504号 0546-8718562'
    # area = match_area.match_address(place=data, key='区')
    # print(area)

    # area_name = match_area.query_belong_province('抚州市')
    # print('***1 area_name ==> ', area_name)

    area1 = '桥西区雅园快捷酒店'
    area_name1 = match_area1.fit_area(area=area1)
    print('area_name1 ==> ', area_name1, area1)

    area2 = '河北省石家庄市桥西区平安南大街139-1号 0311-86986698'
    area_name2 = match_area1.fit_area(area=area2)
    print('area_name2 ==> ', area_name2, area2)

    area3 = '河北银行股份有限公司平南支行  6232656899000192265'
    area_name3 = match_area1.fit_area(area=area3)
    print('area_name3 ==> ', area_name3, area3)

    area_names = []
    if area_name1[0]:
        area_names.append(area_name1)

    if area_name2[0]:
        area_names.append(area_name2)

    if area_name3[0]:
        area_names.append(area_name3)

    # print(area_names)
    # result_area = match_area.opera_areas(area_names)
    # print('*** result_area => ', result_area)

    result_area2 = match_area1.query_sales_address(sales_name=area1, sales_addressphone=area2, sales_bank=area3)
    print('*** result_area2 => ', result_area2)

    # area = match_area.query_province_from_invoice_code('41')
    # print(area)

    # line = '111'
    # output_file = r'/you_filed_algos/prod_kudu_data/abc.txt'
    # save_file(output_file, line, clear_buff=True)
    #
    # content = '四川省成都市锦江区三槐树路3号1层,4至9层'
    # content_trans = transfer_content(content)
    # print(content_trans)
    # print(content.replace(',', ' '))

def demo2():
    pass

if __name__ == '__main__':
    demo2()
