# -*- coding: utf-8 -*-

from report.commons.tools import *
import asyncio, aiomysql


def demo1():
    str1 = '北京市,杭州市,衢州市,郑州市,安庆市,洛阳市'
    str2 = '安庆市'
    print(str1.find(str2))

    # path = "/you_filed_algos/prod_kudu_data/123.txt"
    # content = read_file(path)
    # print(content)

    print('*' * 50)

    sql = """
        UPSERT INTO analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
        SELECT 
        bill_id, 
        '49' as unusual_id,
        company_code,
        account_period,
        account_item,
        finance_number,
        cost_center,
        profit_center,
        '' as cart_head,
        bill_code,
      bill_beg_date,
      bill_end_date,
        ''   as  origin_city,
        ''  as destin_city,
        base_beg_date  as beg_date,
        base_end_date  as end_date,
      apply_emp_name,
        '' as emp_name,
        '' as emp_code,
      '' as company_name,
        0 as jour_amount,
        0 as accomm_amount,
        0 as subsidy_amount,
        0 as other_amount,
        check_amount,
        jzpz,
        '办公费',
        0 as meeting_amount
        FROM 01_datamart_layer_007_h_cw_df.finance_official_bill 
        WHERE bill_id IN ('1','2')
            """

    # print(transfer_content(sql))

    list_info = ['name zhangsan', 'age 10', 'sex man', 'name lisi', 'age 11', 'sex women', 'aaaa', 'bbb', 'ccc']
    ret = list_of_groups(list_info, 5)
    print(ret)

    list2 = ['122', '2333', '3444', ' ', '422', ' ', '    ', '54', ' ', '', None, '   ']
    print(list(filter(not_empty, list2)))  # ['122', '2333', '3444', '422', '54']

    # str1 = '中国建设银行股份有限公司南宁市'
    str1 = '中国工商银行贵阳市'
    print(str1)
    r1 = split_str(str1)
    print('r1 ==> ', r1)

    print('==================' * 10)

    # print('==================' * 10)
    # query_province_city()

    # create_uuid()

    # get_current_time()


def demo2():
    # data = str(input("请输入文本:"))
    # data = "安徽安庆市大观区经三路3号 0556-5386666"
    # data = '江苏省无锡市滨湖区环湖路188号0510'
    data = '山东省东营市东营区开萍乡北二路504号 0546-8718562'
    """
    1，优先找最细的行政单位
    2，加两列，所属省
       出发地所在省份， 目的地所在省份

    """
    province = match_address(place=data, key='乡')
    print('province ==> ', province)
    key = '无锡'
    # print(province.find(key))


def demo3():
    match_area = MatchArea()
    sales_name = '上海澎道投资有限公司'
    sales_addressphone = '上海市金山区前京大道288号'
    sales_bank = '建行上海五角场支行31050175390000000303'

    sales_address = match_area.query_sales_address(sales_name=sales_name, sales_addressphone=sales_addressphone,
                                                   sales_bank=sales_bank)

    print(sales_address)


if __name__ == '__main__':
    demo3()
