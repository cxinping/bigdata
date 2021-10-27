# -*- coding: utf-8 -*-
from report.commons.logging import get_logger
from report.commons.connect_kudu import prod_execute_sql
import uuid
import time
from datetime import datetime, timezone, timedelta

log = get_logger(__name__)



def match_address(place, key):
    ssxq = ['省', '市', '县', '区' , '乡']
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


def get_current_time():
    get_datetime = datetime.now().replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=8)))
    time_str = get_datetime.strftime('%Y-%m-%d %H:%M:%S')
    return time_str



class MatchArea:
    def match_address(self, place, key):
        ssxq = ['省', '市', '县', '区', '乡']
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

    def fit_area(self, area):
        """
        匹配区域到

        """


        
        return None

if __name__ == '__main__':
    """
    1，优先找最细的行政单位
    2，加两列，所属省
       出发地所在省份， 目的地所在省份
        
    origin_province          string        comment "行程出发地(省)",
    destin_province          string        comment "行程目的地(省)",
  
    """

    match_area = MatchArea()
    data = '山东省东营市东营区开萍乡北二路504号 0546-8718562'
    area = match_area.match_address(place=data, key='区')
    print(area)




