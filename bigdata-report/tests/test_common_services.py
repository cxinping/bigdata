# -*- coding: utf-8 -*-
from report.services.common_services import MySQLService
from report.commons.tools import create_uuid


def demo1():
    # for i in range(15):
    #     daily_status = 'ok'
    #     daily_start_date = '2021-11-08 17:05'
    #     daily_end_date = '2021-11-08 20:05'
    #     unusual_point = '2'
    #     daily_source = 'sql'
    #     operate_desc = '1' + str(i)
    #     unusual_infor = 'aaabbbccc'

    # insert_finance_shell_daily(daily_status, daily_start_date, daily_end_date, unusual_point, daily_source,
    #                            operate_desc, unusual_infor)

    # pagination_finance_shell_daily_records(unusual_point='1')

    # unusual_id = '42'
    # category_names = ['d']  # ['a' , 'b']
    # category_classify = '001'
    # update_finance_category_sign(unusual_id, category_names, category_classify)

    # category_classify = '2'
    # records = query_finance_category_sign(unusual_id=unusual_id, category_classify=category_classify)
    # print(records)

    # province_service = ProvinceService()
    # area_name = '金湖县'
    # province_service.query_province(area_name)
    # area_id = '510000'
    # area_id, area_name, parent_id, grade = province_service.query_previous_province(query_area_id=area_id)
    # print(area_id, area_name, parent_id, grade)

    # area_name = '南川区'
    # province_name = province_service.query_belong_province(area_name)
    # print('province_name=',province_name)

    # city_name = province_service.query_receipt_city(area_name='房山区')
    # print(f'city_name={city_name}')

    print('--- ok ---')
    # print('中原区'.find('中'))


def demo2():
    mysql_service = MySQLService()
    #id = create_uuid()
    #mysql_service.insert_update_area(id=id, area_name='盐山县', city='沧州市', province='广东省')

    #id = create_uuid()
    mysql_service.check_area( area_name_val='丰台区', city_val='北京市', province_val=None )

    #id = 'fe5419f562bc4e4fa72b2b8482192614'
    #mysql_service.insert_update_area(id=id, area_name='盐山县', city='111', province='2222')

    # result = mysql_service.query_area_record(area_name='盐山县')
    # print(result)

    print('--- ok ---')


if __name__ == "__main__":
    demo2()




