# -*- coding: utf-8 -*-
from report.commons.logging import get_logger
from report.commons.connect_kudu import prod_execute_sql
import uuid

log = get_logger(__name__)


def match_address(place, key):
    ssxq = ['省', '市', '县', '区']
    indexes0 = [place.find(x) for x in ssxq]
    indexes = [x for x in indexes0 if x > 0]
    indexes2 = sorted(indexes)
    indexes2.insert(0, -1)
    address = []

    # 27个省
    province_ls = ['河北', '山西', '辽宁', '吉林', '黑龙江', '江苏', '浙江', '安徽', '福建', '江西'
        , '山东', '河南', '湖北', '湖南', '广东', '海南', '四川', '贵州', '云南', '陕西', '甘肃', '青海', '台湾']

    if place.find('青岛') > -1:
        return '青岛市'

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


def not_empty(s):
    return s and s.strip()


def split_str(text):
    """
    过滤字符串
    :param text:
    :return:
    """
    result = None

    if text.find('银行') > -1 and text.find('公司') > -1:
        idx = text.find('公司')
        result = text[idx + 2:]
    elif text.find('银行') > -1:
        idx = text.find('银行')
        result = text[idx + 2:]
    elif text.find('局') > -1:
        idx = text.find('局')
        result = text[idx + 1:]
    elif text.find('公司') > -1:
        idx = text.find('公司')
        result = text[idx + 2:]
    else:
        result = text
    return result


def query_province_city():
    select_sql = "select area_id,area_name,parent_id,grade from 01_datamart_layer_007_h_cw_df.finance_province_city where grade = '1'"
    # 查询省级地区
    finance_province_city_lev1 = prod_execute_sql(conn_type='test', sqltype='select', sql=select_sql)
    print(finance_province_city_lev1)

    # 查询市级地区



    # 查询县级地区


def create_uuid():
    uuid_str = str(uuid.uuid4())
    suid = ''.join(uuid_str.split('-'))
    return suid



if __name__ == '__main__':
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

    # data = str(input("请输入文本:"))
    # data = "安徽安庆市大观区经三路3号 0556-5386666"
    # data = '江苏省无锡市滨湖区环湖路188号0510'
    data = '山东省东营市东营区北二路504号 0546-8718562'
    province = match_address(place=data, key='市')
    print(province)
    key = '无锡'
    print(province.find(key))

    print('==================' * 10 )
    #query_province_city()

    create_uuid()




