# -*- coding: utf-8 -*-
from report.commons.logging import get_logger
from report.commons.connect_kudu import prod_execute_sql
import uuid
import time
from datetime import datetime, timezone, timedelta
import psutil

log = get_logger(__name__)


def kill_pid(parent_pid):
    parent = psutil.Process(parent_pid)
    for child in parent.children(recursive=True):  # or parent.children() for recursive=False
        child.kill()
    parent.kill()


def match_address(place, key):
    ssxq = ['省', '市', '县', '区', '乡']
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


def transfer_content(content):
    """
    处理字符转转义，用于insert sql语句
    :param content:
    :return:
    """
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
    """
    :param list_info:   列表
    :param per_list_len:  每个小列表的长度
    :return:
    """
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

        if place is None:
            return None

        place = place.replace('市区', '市')

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

    def opera_areas(self, ares):
        max_level_area, result_area_name = 0, None
        for idx, area in enumerate(ares):
            area_name, area_level = area[0], area[1]

            if area_level == 3:
                return area_name
            elif area_level == 2:
                return area_name
            elif area_level == 1:
                return area_name

        return result_area_name

    def opera_areas2(self, ares):
        max_level_area, result_area_name = 0, None
        for idx, area in enumerate(ares):
            area_name, area_level = area[0], area[1]

            if idx == 0:
                max_level_area = area_level
                result_area_name = area_name
            elif area_level > max_level_area:
                max_level_area = area_level
                result_area_name = area_name

        # print('*** result_area_name ==> ', result_area_name, max_level_area )
        return result_area_name

    def fit_area(self, area):
        """
        匹配区域到最细的行政区域

        """

        if area is None or area == 'None':
            return None, -1

        # 县, 区
        county = self.match_address(place=area, key='县') if self.match_address(place=area,
                                                                               key='县') else self.match_address(
            place=area, key='区')

        if county:
            area_id, area_name, parent_id, grade = self._query_province(county)
            # print('&&&&& county==> ' , county, area, grade)

            if grade is None:
                return None, -1

            return county, int(grade)
        else:
            # 市
            city = self.match_address(place=area, key='市')
            if city:
                area_id, area_name, parent_id, grade = self._query_province(city)

                # print('&&&&& city==> ' , city, area, grade)

                if len(city) > 5 or grade is None:
                    return None, -1

                return city, int(grade)
            else:
                # 省
                province = self.match_address(place=area, key='省')

                if province:
                    return province, 1

        return None, -1

    def query_destin_province(self, invo_code, destin_name):
        """
        根据发票代码前两位找行程目的地所属省，若是没有发票，再根据行程目的地找所属省
        """

        province = None
        if invo_code is None:
            if destin_name and destin_name.find(',') != -1:
                province = self.query_belong_province(destin_name)

        else:
            invo_code = invo_code[0:2]
            province = self.query_province_from_invoice_code(invo_code)

        return province

    def query_belong_province(self, keyword):
        """
        查找关键词所在的省
        :param keyword:
        :return:
        """

        if keyword is None:
            return None

        area_id, area_name, parent_id, grade = self._query_province(keyword)
        # print(area_id, area_name, parent_id, grade)

        if area_name is None or area_name == 'None':
            return None

        idx = 0
        if grade and grade != '1':

            while grade and grade != '1':
                idx = idx + 1

                if idx > 3:
                    return None

                area_id, area_name, parent_id, grade = self._query_previous_province(area_id=parent_id)
                if grade and grade == '1':
                    return area_name

        elif grade and grade == '1':
            return area_name

        return None

    def _query_previous_province(self, area_id):
        """
        查找上一级的行政区域
        :param area_id:
        :return:
        """

        if area_id is None:
            return None, None, None, None

        try:
            sel_sql = f"select area_id, area_name, parent_id, grade from 01_datamart_layer_007_h_cw_df.finance_province_city where area_id = '{area_id}'"
            # print(sel_sql)
            records = prod_execute_sql(conn_type='test', sqltype='select', sql=sel_sql)

            if len(records) > 0:
                record = records[0]
                area_id = str(record[0]) if record[0] else None
                area_name = str(record[1]) if record[1] else None
                parent_id = str(record[2]) if record[2] else None
                grade = str(record[3]) if record[3] else None

                return area_id, area_name, parent_id, grade

            return None, None, None, None
        except Exception as e:
            print(e)
            return None, None, None, None

    def _query_province(self, keyword):

        if keyword is None or keyword == 'None':
            return None, None, None, None

        try:
            # sel_sql = f"select area_id, area_name, parent_id, grade from 01_datamart_layer_007_h_cw_df.finance_province_city where area_name like '%{keyword}%'"
            sel_sql = f"select area_id, area_name, parent_id, grade from 01_datamart_layer_007_h_cw_df.finance_province_city where area_name = '{keyword}'"
            records = prod_execute_sql(conn_type='test', sqltype='select', sql=sel_sql)

            if len(records) > 0:
                record = records[0]
                area_id = str(record[0]) if record[0] else None
                area_name = str(record[1]) if record[1] else None
                parent_id = str(record[2]) if record[2] else None
                grade = str(record[3]) if record[3] else None

                return area_id, area_name, parent_id, grade

            return None, None, None, None
        except Exception as e:
            print(e)
            return None, None, None, None

    def query_receipt_city(self, sales_name, sales_addressphone, sales_bank):
        """
        查询发票开票所在市
        :param sales_name: 开票公司
        :param sales_addressphone: 开票地址及电话
        :param sales_bank: 发票开户行
        :return:
        """

        area_name1, area_name2, area_name3 = None, None, None
        if sales_name != 'None' or sales_name is not None:
            area_name1 = self.match_address(place=sales_name, key='市')
            return area_name1
        elif sales_addressphone != 'None' or sales_addressphone is not None:
            area_name2 = self.match_address(place=sales_addressphone, key='市')
            return area_name2
        elif sales_bank != 'None' or sales_bank is not None:
            area_name3 = self.match_address(place=sales_bank, key='市')
            return area_name3
        else:
            return None

    def query_sales_address(self, sales_name, sales_addressphone, sales_bank):
        """
        查询发票开票所在市
        :param sales_name: 开票公司
        :param sales_addressphone: 开票地址及电话
        :param sales_bank: 发票开户行
        :return:
        """

        # print('sales_name=', sales_name)
        # print('sales_addressphone=', sales_addressphone)
        # print('sales_bank=', sales_bank)

        # sales_name = sales_name.replace('超市', '')
        # sales_addressphone = sales_addressphone.replace('超市', '')
        # sales_bank = sales_bank.replace('超市', '')

        area_name1, area_name2, area_name3 = None, None, None
        if sales_name != 'None' or sales_name is not None:
            area_name1 = self.fit_area(area=sales_name)

        if sales_addressphone != 'None' or sales_addressphone is not None:
            area_name2 = self.fit_area(area=sales_addressphone)

        if sales_bank != 'None' or sales_bank is not None:
            area_name3 = self.fit_area(area=sales_bank)

        area_names = []
        if area_name1[0]:
            area_names.append(area_name1)

        if area_name2[0]:
            area_names.append(area_name2)

        if area_name3[0]:
            area_names.append(area_name3)

        result_area = self.opera_areas(area_names)

        # show_str = f'{sales_name} , {sales_addressphone} , {sales_bank}, {result_area}'
        # print('### query_sales_address show_str ==> ', show_str)

        return result_area

    def query_province_from_invoice_code(self, invo_code_2_letter):
        """
        根据发票代码前两位找行程目的地所属省，若是没有发票，再根据行程目的地找所属省
        :return:
        """

        province = ''
        if invo_code_2_letter == '11':
            province = '北京市'
        elif invo_code_2_letter == '12':
            province = '天津市'
        elif invo_code_2_letter == '13':
            province = '河北省'
        elif invo_code_2_letter == '14':
            province = '山西省'
        elif invo_code_2_letter == '15':
            province = '内蒙古自治区'
        elif invo_code_2_letter == '21':
            province = '辽宁省'
        elif invo_code_2_letter == '22':
            province = '吉林省'
        elif invo_code_2_letter == '23':
            province = '黑龙江省'
        elif invo_code_2_letter == '31':
            province = '上海市'
        elif invo_code_2_letter == '32':
            province = '江苏省'
        elif invo_code_2_letter == '33':
            province = '浙江省'
        elif invo_code_2_letter == '34':
            province = '安徽省'
        elif invo_code_2_letter == '35':
            province = '福建省'
        elif invo_code_2_letter == '36':
            province = '江西省'
        elif invo_code_2_letter == '37':
            province = '山东省'
        elif invo_code_2_letter == '41':
            province = '河南省'
        elif invo_code_2_letter == '42':
            province = '湖北省'
        elif invo_code_2_letter == '43':
            province = '湖南省'
        elif invo_code_2_letter == '44':
            province = '广东省'
        elif invo_code_2_letter == '45':
            province = '广西壮族自治区'
        elif invo_code_2_letter == '46':
            province = '海南省'
        elif invo_code_2_letter == '50':
            province = '重庆市'
        elif invo_code_2_letter == '51':
            province = '四川省'
        elif invo_code_2_letter == '52':
            province = '贵州省'
        elif invo_code_2_letter == '53':
            province = '云南省'
        elif invo_code_2_letter == '54':
            province = '西藏自治区'
        elif invo_code_2_letter == '61':
            province = '陕西省'
        elif invo_code_2_letter == '62':
            province = '甘肃省'
        elif invo_code_2_letter == '63':
            province = '青海省'
        elif invo_code_2_letter == '64':
            province = '宁夏回族自治区'
        elif invo_code_2_letter == '65':
            province = '新疆维吾尔自治区'
        elif invo_code_2_letter == '71':
            province = '台湾省'
        elif invo_code_2_letter == '81':
            province = '香港特别行政区'
        elif invo_code_2_letter == '82':
            province = '澳门特别行政区'
        return province


result = []


def save_file(output_file, line, buff_size=1000, clear_buff=False):
    global result

    if line:
        result.append(line)

    if len(result) >= buff_size or clear_buff:
        with open(output_file, "a+") as fp:
            fp.write("\n".join(result))
            fp.write("\n")
        result = []
