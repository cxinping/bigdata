# -*- coding: utf-8 -*-
import time
import datetime
import re
from report.commons.logging import get_logger

log = get_logger(__name__)


def match_address(place, key):
    ssxq = ['省', '市', '县', '区']
    indexes0 = [place.find(x) for x in ssxq]
    indexes = [x for x in indexes0 if x > 0]
    indexes2 = sorted(indexes)
    indexes2.insert(0, -1)
    address = []

    for i, x in enumerate(indexes2):
        if i == len(indexes2) - 1:
            continue
        else:
            address.append(place[x + 1:indexes2[i + 1] + 1])

    for addr in address:
        if addr.find(key) > -1:
            return addr
    return None


def check_invoicing_place(addr1, addr2):
    if addr1.find(addr2) > -1 or addr2.find(addr1):
        return True

    return False


def read_file(path):
    with open(path, "r") as f:
        data = f.read()
        return data


#   处理字符转转义，用于insert sql语句
def transfer_content(content):
    if content is None:
        return None
    else:
        string = ""
        for c in content:
            if c == '"':
                string += '\\\"'
            elif c == "'":
                string += "\\\'"
            elif c == "\\":
                string += "\\\\"
            elif c == ":":  # 冒号也要转义，否则报错
                string += "\\:"
            else:
                string += c
        return string


def list_of_groups(list_info, per_list_len):
    '''
    :param list_info:   列表
    :param per_list_len:  每个小列表的长度
    :return:
    '''
    list_of_group = zip(*(iter(list_info),) * per_list_len)
    end_list = [list(i) for i in list_of_group]  # i is a tuple
    count = len(list_info) % per_list_len
    end_list.append(list_info[-count:]) if count != 0 else end_list
    return end_list


if __name__ == '__main__':
    # data = str(input("请输入文本:"))
    # data = "安徽安庆市大观区经三路3号 0556-5386666"
    #data = '江苏省无锡市滨湖区环湖路188号0510'
    data = '山东省东营市东营区北二路504号 0546-8718562'
    province = match_address(place=data, key='市')
    print(province)
    key = '无锡'
    print(province.find(key))

    str1 = '北京市,杭州市,衢州市,郑州市,安庆市,洛阳市'
    str2 = '安庆市'
    print(str1.find(str2))

    path = "/you_filed_algos/prod_kudu_data/123.txt"
    content = read_file(path)
    print(content)

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

    print(transfer_content(sql))

    list_info = ['name zhangsan', 'age 10', 'sex man', 'name lisi', 'age 11', 'sex women', 'aaaa', 'bbb', 'ccc']
    ret = list_of_groups(list_info, 5)
    print(ret)
