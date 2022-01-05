# -*- coding: utf-8 -*-
from report.commons.tools import *
from report.commons.commons import get_date_month
import re


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
    list_info = ['a', 'b', 'c', 'd', 'e', 'f', 'g']
    rs_ls = list_of_groups(list_info=list_info, per_list_len=3)
    # print(rs_ls)

    for idx, group in enumerate(rs_ls):
        print(idx, group)


def demo3():
    match_area = MatchArea()
    sales_name = '广西嘉旸碧天酒店管理有限公司'
    sales_addressphone = '广西南宁市吴圩国际机场T2航站区机场大道18号旅客过夜用房0771-2883346'
    sales_bank = '中国银行南宁市机场支行611974862198'
    receipt_city = match_area.query_receipt_city_new(sales_name=sales_name, sales_addressphone=sales_addressphone,
                                                     sales_bank=sales_bank)
    print(f'receipt_city={receipt_city}')


def demo4():
    date = get_date_month(1)
    print(date)


def demo5():
    # ret1 = is_chinese("刘亦菲")
    ret1 = is_chinese("aaaa111")
    print(ret1, not ret1)


def demo6():
    str1 = ',今天111222223333#'
    str2 = '，4444456666667777测试'
    # totalNumbers = re.findall(r'\d+', str2)
    # print(totalNumbers)

    totalNumbers = filter_numbers(str1)
    print(totalNumbers)


if __name__ == '__main__':
    # demo2()
    # demo3()
    # demo5()
    #demo6()

    demo4()

    print('--- ok ---')
