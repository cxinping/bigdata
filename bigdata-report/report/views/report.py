# -*- coding: utf-8 -*-

'''
Created on 2021-08-02

@author: WangShuo
'''

from concurrent.futures import ThreadPoolExecutor

import datetime
import json
from flask import Blueprint, jsonify, request, make_response

from report.commons.connect_kudu import prod_execute_sql
from report.commons.logging import get_logger
from report.commons.tools import transfer_content
from report.services.office_expenses_service import query_checkpoint_42_commoditynames, get_office_bill_jiebaword, \
    pagination_office_records
from report.services.vehicle_expense_service import query_checkpoint_55_commoditynames, get_car_bill_jiebaword, \
    pagination_car_records
from report.commons.tools import get_current_time
from report.services.common_services import (insert_finance_shell_daily, update_finance_category_sign,
                                             query_finance_category_sign)
from report.services.conference_expense_service import pagination_conference_records, get_conference_bill_jiebaword, \
    pagination_conference_records, query_checkpoint_26_commoditynames
from report.commons.db_helper import Pagination

log = get_logger(__name__)
report_bp = Blueprint('report', __name__)


# http://10.5.138.11:8004/report/test/wang
@report_bp.route('/test/<name>', methods=['GET', 'POST'])
def test_report(name):
    log.info('---- test_report ============')
    gender = None
    if request.method == "POST":
        gender = request.form['gender']
        log.info(f'gender={gender}')

    elif request.method == "GET":
        address = request.args.get("address")
        log.info(f'address={address}')

    data_ls = [{'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e': 5}]
    data_str = json.dumps({'a': 'Runoob', 'b': 7}, sort_keys=True, indent=4, separators=(',', ': '))

    result = {
        'name': name,
        'time': datetime.datetime.now(),
        'gender': gender if gender else '',
        'code': '001',
        'data': data_ls
    }

    response = jsonify(result)
    return response, 200


# http://10.5.138.11:8004/report/var
@report_bp.route('/var/<name>', methods=['GET', 'POST'])
def test_var(name):
    log.info('---- test_report ============')
    gender = None
    if request.method == "POST":
        gender = request.form['gender']
        log.info(f'gender={gender}')

    elif request.method == "GET":
        address = request.args.get("address")
        log.info(f'address={address}')

    data_ls = [{'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e': 5}]
    data_str = json.dumps({'a': 'Runoob', 'b': 7}, sort_keys=True, indent=4, separators=(',', ': '))

    result = {
        'name': name,
        'time': datetime.datetime.now(),
        'gender': gender if gender else '',
        'code': '001',
        'data': data_ls
    }

    response = jsonify(result)
    return response, 200


############  【费用标准（finance_standard）相关】  ############

# http://10.5.138.11:8004/report/finance_standard/add
@report_bp.route('/finance_standard/add', methods=['POST'])
def finance_standard_add():
    log.info('----- finance_standard add -----')
    standard_id = request.form.get('standard_id') if request.form.get('standard_id') else None
    unusual_id = request.form.get('unusual_id') if request.form.get('unusual_id') else None
    unusual_level = request.form.get('unusual_level') if request.form.get('unusual_level') else None
    standard_value = request.form.get('standard_value') if request.form.get('standard_value') else 0
    out_value = request.form.get('out_value') if request.form.get('out_value') else 0

    log.info(f'standard_id={standard_id}')
    log.info(f'unusual_id={unusual_id}')
    log.info(f'unusual_level={unusual_level}')
    log.info(f'standard_value={standard_value}')
    log.info(f'out_value={out_value}')

    if standard_id is None:
        data = {"result": "error", "details": "输入的 standard_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    sql = f"""
insert into 01_datamart_layer_007_h_cw_df.finance_standard(standard_id,unusual_id,unusual_level,standard_value,out_value) 
values('{standard_id}','{unusual_id}','{unusual_level}',{standard_value},{out_value}) 
    """.replace('\n', '')

    print(sql)
    try:
        prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)
        data = {
            'result': 'ok',
            'code': 200,
            'details': '成功新增一条"费用标准"记录'
        }
        response = jsonify(data)
        return response
    except Exception as e:
        print(e)
        data = {
            'result': 'error',
            'code': 500,
            'details': str(e)
        }
        response = jsonify(data)
        return response


# http://10.5.138.11:8004/report/finance_standard/update
@report_bp.route('/finance_standard/update', methods=['POST'])
def finance_standard_update():
    log.info('----- finance_standard update -----')
    standard_id = request.form.get('standard_id') if request.form.get('standard_id') else None
    unusual_id = request.form.get('unusual_id') if request.form.get('unusual_id') else None
    unusual_level = request.form.get('unusual_level') if request.form.get('unusual_level') else None
    standard_value = request.form.get('standard_value') if request.form.get('standard_value') else 0
    out_value = request.form.get('out_value') if request.form.get('out_value') else 0

    log.info(f'standard_id={standard_id}')
    log.info(f'unusual_id={unusual_id}')
    log.info(f'unusual_level={unusual_level}')
    log.info(f'standard_value={standard_value}')
    log.info(f'out_value={out_value}')

    if standard_id is None:
        data = {"result": "error", "details": "输入的 standard_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    sql = f"""
    update 01_datamart_layer_007_h_cw_df.finance_standard set unusual_id='{unusual_id}', unusual_level='{unusual_level}',standard_value={standard_value}, out_value={out_value}
    where standard_id='{standard_id}'
    """.replace('\n', '')

    print(sql)
    try:
        prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)

        data = {
            'result': 'ok',
            'code': 200,
            'details': '成功修改一条"费用标准"记录'
        }

        response = jsonify(data)
        return response
    except Exception as e:
        print(e)
        data = {
            'result': 'error',
            'code': 500,
            'details': str(e)
        }
        response = jsonify(data)
        return response


# http://10.5.138.11:8004/report/finance_standard/delete
@report_bp.route('/finance_standard/delete', methods=['POST'])
def finance_standard_delete():
    log.info('----- finance_standard delete -----')
    standard_id = request.form.get('standard_id') if request.form.get('standard_id') else None
    log.info(f'standard_id={standard_id}')

    if standard_id is None:
        data = {"result": "error", "details": "输入的 standard_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    sql = f"""
delete from  01_datamart_layer_007_h_cw_df.finance_standard where standard_id='{standard_id}'
    """.replace('\n', '')
    print(sql)

    try:
        prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)

        data = {
            'result': 'ok',
            'code': 200,
            'details': '成功删除一条"费用标准"记录'
        }

        response = jsonify(data)
        return response
    except Exception as e:
        print(e)
        data = {
            'result': 'error',
            'code': 500,
            'details': str(e)
        }
        response = jsonify(data)
        return response


############  风景名胜信息 行管  ############

# http://10.5.138.11:8004/report/finance_scenery/add
@report_bp.route('/finance_scenery/add', methods=['POST'])
def finance_scenery_add():
    log.info('----- finance_scenery add -----')
    scenery_id = request.form.get('scenery_id') if request.form.get('scenery_id') else None
    scenery_name = request.form.get('scenery_name') if request.form.get('scenery_name') else None
    province = request.form.get('province') if request.form.get('province') else None
    city = request.form.get('city') if request.form.get('city') else None
    county = request.form.get('county') if request.form.get('county') else None
    address = request.form.get('address') if request.form.get('address') else None

    log.info(f'scenery_id={scenery_id}')
    log.info(f'scenery_name={scenery_name}')
    log.info(f'province={province}')
    log.info(f'city={city}')
    log.info(f'county={county}')
    log.info(f'address={address}')

    if scenery_id is None:
        data = {"result": "error", "details": "输入的 scenery_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    sql = f"""
    insert into 01_datamart_layer_007_h_cw_df.finance_scenery(scenery_id, scenery_name, province, city, county , address)
    values('{scenery_id}','{scenery_name}','{province}', '{city}', '{county}', '{address}')
        """.replace('\n', '')

    print(sql)
    try:
        prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)
        data = {
            'result': 'ok',
            'code': 200,
            'details': '成功新增一条"风景名胜"记录'
        }
        response = jsonify(data)
        return response
    except Exception as e:
        print(e)
        data = {
            'result': 'error',
            'code': 500,
            'details': str(e)
        }
        response = jsonify(data)
        return response


# http://10.5.138.11:8004/report/finance_scenery/update
@report_bp.route('/finance_scenery/update', methods=['POST'])
def finance_scenery_update():
    log.info('----- finance_scenery update -----')
    scenery_id = request.form.get('scenery_id') if request.form.get('scenery_id') else None
    scenery_name = request.form.get('scenery_name') if request.form.get('scenery_name') else None
    province = request.form.get('province') if request.form.get('province') else None
    city = request.form.get('city') if request.form.get('city') else None
    county = request.form.get('county') if request.form.get('county') else None
    address = request.form.get('address') if request.form.get('address') else None

    log.info(f'scenery_id={scenery_id}')
    log.info(f'scenery_name={scenery_name}')
    log.info(f'province={province}')
    log.info(f'city={city}')
    log.info(f'county={county}')
    log.info(f'address={address}')

    if scenery_id is None:
        data = {"result": "error", "details": "输入的 scenery_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    sql = f"""
    update 01_datamart_layer_007_h_cw_df.finance_scenery set scenery_name='{scenery_name}', province='{province}', city='{city}',county='{county}', address='{address}'
    where scenery_id='{scenery_id}'
        """.replace('\n', '')

    print(sql)
    try:
        prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)
        data = {
            'result': 'ok',
            'code': 200,
            'details': '成功修改一条"风景名胜"记录'
        }
        response = jsonify(data)
        return response
    except Exception as e:
        print(e)
        data = {
            'result': 'error',
            'code': 500,
            'details': str(e)
        }
        response = jsonify(data)
        return response


# http://10.5.138.11:8004/report/finance_scenery/delete
@report_bp.route('/finance_scenery/delete', methods=['POST'])
def finance_scenery_delete():
    log.info('----- finance_scenery  delete -----')
    scenery_id = request.form.get('scenery_id') if request.form.get('scenery_id') else None
    log.info(f'scenery_id={scenery_id}')

    if scenery_id is None:
        data = {"result": "error", "details": "输入的 scenery_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    sql = f"""
    delete from  01_datamart_layer_007_h_cw_df.finance_scenery where scenery_id='{scenery_id}' 
        """.replace('\n', '')

    try:
        prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)
        data = {
            'result': 'ok',
            'code': 200,
            'details': '成功删除一条"风景名胜"记录'
        }
        response = jsonify(data)
        return response
    except Exception as e:
        print(e)
        data = {
            'result': 'error',
            'code': 500,
            'details': str(e)
        }
        response = jsonify(data)
        return response


############  【集团股份人数（finance_person）相关】  ############

# http://10.5.138.11:8004/report/finance_person/add
@report_bp.route('/finance_person/add', methods=['POST'])
def finance_person_add():
    log.info('----- finance_person add -----')
    person_id = request.form.get('person_id') if request.form.get('person_id') else None
    company_code = request.form.get('company_code') if request.form.get('company_code') else None
    inner_code = request.form.get('inner_code') if request.form.get('inner_code') else None
    sum_person = request.form.get('sum_person') if request.form.get('sum_person') else 0
    pesiod = request.form.get('pesiod') if request.form.get('pesiod') else None

    log.info(f'person_id={person_id}')
    log.info(f'company_code={company_code}')
    log.info(f'inner_code={inner_code}')
    log.info(f'sum_person={sum_person}')
    log.info(f'pesiod={pesiod}')

    if person_id is None:
        data = {"result": "error", "details": "输入的 person_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    try:
        sql = f"""
        insert into 01_datamart_layer_007_h_cw_df.finance_person(person_id, company_code, inner_code, sum_person, pesiod )
        values('{person_id}','{company_code}','{inner_code}', {sum_person}, '{pesiod}' )
            """.replace('\n', '')
        print(sql)
        prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)

        data = {
            'result': 'ok',
            'code': 200,
            'details': '成功新增一条"集团和股份公司人数"记录'
        }
        response = jsonify(data)
        return response
    except Exception as e:
        print(e)
        data = {
            'result': 'error',
            'code': 500,
            'details': str(e)
        }
        response = jsonify(data)
        return response


# http://10.5.138.11:8004/report/finance_person/update
@report_bp.route('/finance_person/update', methods=['POST'])
def finance_person_update():
    log.info('----- finance_person update -----')
    person_id = request.form.get('person_id') if request.form.get('person_id') else None
    company_code = request.form.get('company_code') if request.form.get('company_code') else None
    inner_code = request.form.get('inner_code') if request.form.get('inner_code') else None
    sum_person = request.form.get('sum_person') if request.form.get('sum_person') else 0
    pesiod = request.form.get('pesiod') if request.form.get('pesiod') else None

    log.info(f'person_id={person_id}')
    log.info(f'company_code={company_code}')
    log.info(f'inner_code={inner_code}')
    log.info(f'sum_person={sum_person}')
    log.info(f'pesiod={pesiod}')

    if person_id is None:
        data = {"result": "error", "details": "输入的 person_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    sql = f"""
    update 01_datamart_layer_007_h_cw_df.finance_person set company_code='{company_code}', inner_code='{inner_code}', sum_person={sum_person}, pesiod='{pesiod}'
    where person_id = '{person_id}'
        """.replace('\n', '')

    print(sql)
    try:
        prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)
        data = {
            'result': 'ok',
            'code': 200,
            'details': "成功修改一条'集团和股份公司人数'记录"
        }
        response = jsonify(data)
        return response
    except Exception as e:
        print(e)
        data = {
            'result': 'error',
            'code': 500,
            'details': str(e)
        }
        response = jsonify(data)
        return response


# http://10.5.138.11:8004/report/finance_person/delete
@report_bp.route('/finance_person/delete', methods=['POST'])
def finance_person_delete():
    log.info('----- finance_scenery delete -----')
    person_id = request.form.get('person_id') if request.form.get('person_id') else None
    log.info(f'person_id={person_id}')

    if person_id is None:
        data = {"result": "error", "details": "输入的 person_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    sql = f"""
    delete from 01_datamart_layer_007_h_cw_df.finance_person where person_id='{person_id}' 
        """.replace('\n', '')

    try:
        prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)
        data = {
            'result': 'ok',
            'code': 200,
            'details': '成功删除一条风景名胜记录'
        }
        response = jsonify(data)
        return response
    except Exception as e:
        print(e)
        data = {
            'result': 'error',
            'code': 500,
            'details': str(e)
        }
        response = jsonify(data)
        return response


############  【检查点（finance_unusual）相关】  ############

# http://10.5.138.11:8004/report/finance_unusual/add
@report_bp.route('/finance_unusual/add', methods=['POST'])
def finance_unusual_add():
    log.info('----- finance_unusual add -----')
    unusual_id = request.form.get('unusual_id') if request.form.get('unusual_id') else None
    cost_project = request.form.get('cost_project') if request.form.get('cost_project') else None
    unusual_number = request.form.get('unusual_number') if request.form.get('unusual_number') else None
    number_name = request.form.get('number_name') if request.form.get('number_name') else None
    unusual_type = request.form.get('unusual_type') if request.form.get('unusual_type') else None
    unusual_point = request.form.get('unusual_point') if request.form.get('unusual_point') else None
    unusual_content = request.form.get('unusual_content') if request.form.get('unusual_content') else None
    unusual_shell = request.form.get('unusual_shell') if request.form.get('unusual_shell') else None
    isalgorithm = request.form.get('isalgorithm') if request.form.get('isalgorithm') else None

    log.info(f'unusual_id={unusual_id}')
    log.info(f'cost_project={cost_project}')
    log.info(f'unusual_number={unusual_number}')
    log.info(f'number_name={number_name}')
    log.info(f'unusual_type={unusual_type}')
    log.info(f'unusual_point={unusual_point}')
    log.info(f'unusual_content={unusual_content}')
    log.info(f'unusual_shell={unusual_shell}')
    log.info(f'isalgorithm={isalgorithm}')

    if unusual_id is None:
        data = {"result": "error", "details": "输入的 unusual_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    sql = f"""
insert into 01_datamart_layer_007_h_cw_df.finance_unusual(unusual_id ,cost_project, unusual_number, number_name, unusual_type, unusual_point, unusual_content, unusual_shell, isalgorithm)
values('{unusual_id}','{cost_project}','{unusual_number}','{number_name}' ,'{unusual_type}', '{unusual_point}' , '{unusual_content}', "{unusual_shell}", '{isalgorithm}')
    """.replace('\n', '')

    print(sql)
    try:
        prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)

        data = {
            'result': 'ok',
            'code': 200,
            'details': '成功新增一条"检查点相关"记录'
        }
        response = jsonify(data)
        return response
    except Exception as e:
        print(e)
        data = {
            'result': 'error',
            'code': 500,
            'details': str(e)
        }
        response = jsonify(data)
        return response


# http://10.5.138.11:8004/report/finance_unusual/update
@report_bp.route('/finance_unusual/update', methods=['POST'])
def finance_unusual_update():
    log.info('----- finance_unusual update -----')
    unusual_id = request.form.get('unusual_id') if request.form.get('unusual_id') else None
    unusual_point = request.form.get('unusual_point') if request.form.get('unusual_point') else None
    unusual_content = request.form.get('unusual_content') if request.form.get('unusual_content') else None
    unusual_shell = request.form.get('unusual_shell') if request.form.get('unusual_shell') else None
    # 1为sql类2为算法类
    isalgorithm = request.form.get('unusual_shell') if request.form.get('unusual_shell') else None

    log.info(f'unusual_id={unusual_id}')
    log.info(f'unusual_point={unusual_point}')
    log.info('* unusual_content *')
    # log.info(f'unusual_shell={unusual_shell}')

    unusual_shell = transfer_content(unusual_shell)
    print(unusual_shell)

    if unusual_id is None:
        data = {"result": "error", "details": "输入的 unusual_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if unusual_point is None:
        data = {"result": "error", "details": "输入的 unusual_point 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if unusual_content is None:
        data = {"result": "error", "details": "输入的 unusual_content 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if unusual_shell is None:
        data = {"result": "error", "details": "输入的 unusual_shell 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if isalgorithm is None:
        data = {"result": "error", "details": "输入的 isalgorithm 不能为空", "code": 500}
        response = jsonify(data)
        return response

    sql = f"""
    update 01_datamart_layer_007_h_cw_df.finance_unusual set unusual_point='{unusual_point}', unusual_content='{unusual_content}', unusual_shell="{unusual_shell}", isalgorithm="{isalgorithm}"
    where unusual_id='{unusual_id}'
    """  # .replace('\n', '').replace('\r', '').strip()

    print(sql)

    try:
        prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)

        data = {
            'result': 'ok',
            'code': 200,
            'details': "成功修改一条'检查点相关'记录"
        }
        response = jsonify(data)
        return response
    except Exception as e:
        print(e)
        data = {
            'result': 'error',
            'code': 500,
            'details': str(e)
        }
        response = jsonify(data)
        return response


# http://10.5.138.11:8004/report/finance_unusual/delete
@report_bp.route('/finance_unusual/delete', methods=['POST'])
def finance_unusual_delete():
    log.info('----- finance_unusual delete -----')
    unusual_id = request.form.get('unusual_id') if request.form.get('unusual_id') else None

    if unusual_id is None:
        data = {"result": "error", "details": "输入的 unusual_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    try:
        sql = f"""
        delete from 01_datamart_layer_007_h_cw_df.finance_unusual where unusual_id='{unusual_id}'
            """.replace('\n', '')
        print(sql)

        prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)
        data = {
            'result': 'ok',
            'code': 200,
            'details': '成功删除一条"检查点相关"记录'
        }
        response = jsonify(data)
        return response
    except Exception as e:
        print(e)
        data = {
            'result': 'error',
            'code': 500,
            'details': str(e)
        }
        response = jsonify(data)
        return response


executor = ThreadPoolExecutor(2)


# http://10.5.138.11:8004/report/finance_unusual/execute
@report_bp.route('/finance_unusual/execute', methods=['POST'])
def finance_unusual_execute():
    log.info('----- finance_unusual execute -----')
    unusual_id = request.form.get('unusual_id') if request.form.get('unusual_id') else None

    if unusual_id is None:
        data = {"result": "error", "details": "输入的 unusual_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    try:
        sql = f"""
            select unusual_id,unusual_shell,isalgorithm from 01_datamart_layer_007_h_cw_df.finance_unusual where unusual_id='{unusual_id}'
                """.replace('\n', '')
        print(sql)
        result = prod_execute_sql(conn_type='test', sqltype='select', sql=sql)
        unusual_shell = str(result[0][1])
        # "1为sql类, 2为算法类",
        isalgorithm = str(result[0][2])

        if unusual_shell is None:
            data = {"result": "error", "details": f"查询的 {unusual_id} 对应的 unusual_shell 不能为空", "code": 500}
            response = jsonify(data)
            return response

        if isalgorithm is None:
            data = {"result": "error", "details": f"查询的 {unusual_id} 对应的 isalgorithm 不能为空", "code": 500}
            response = jsonify(data)
            return response

        ######### 执行 SQL ############
        if isalgorithm == '1':
            executor.submit(execute_kudu_sql, unusual_shell, unusual_id)

        elif isalgorithm == '2':
            ###### 执行算法 python 脚本  ############
            # eval("print(1+2)")
            print(unusual_shell)
            exec("print('执行算法 shell 开始')")
            daily_start_date = get_current_time()

            exec(unusual_shell, globals())
            exec("print('执行算法 shell 结束')")
            daily_end_date = get_current_time()

            insert_finance_shell_daily(daily_status='ok', daily_start_date=daily_start_date,
                                       daily_end_date=daily_end_date, unusual_point=unusual_id, daily_source='sql',
                                       operate_desc='', unusual_infor='')

        data = {
            'result': 'ok',
            'code': 200,
            'details': f'执行检查点{unusual_id}的SQL或Python Shell'
        }
        response = jsonify(data)
        return response
    except Exception as e:
        print(e)
        data = {
            'result': 'error',
            'code': 500,
            'details': str(e)
        }

        daily_source = 'sql' if isalgorithm == '1' else 'python shell'
        insert_finance_shell_daily(daily_status='error', daily_start_date=daily_start_date,
                                   daily_end_date=daily_end_date,
                                   unusual_point=unusual_id, daily_source=daily_source, operate_desc='',
                                   unusual_infor=str(e))

        response = jsonify(data)
        return response


def execute_kudu_sql(unusual_shell, unusual_id):
    print(unusual_shell)

    try:
        daily_start_date = get_current_time()
        print('*** begin execute_kudu_sql ')
        prod_execute_sql(conn_type='test', sqltype='insert', sql=unusual_shell)
        daily_end_date = get_current_time()
        operate_desc = f'成功执行检查点{unusual_id}的SQL'
        print('*** end execute_kudu_sql ***')

        insert_finance_shell_daily(daily_status='ok', daily_start_date=daily_start_date, daily_end_date=daily_end_date,
                                   unusual_point=unusual_id, daily_source='sql', operate_desc=operate_desc,
                                   unusual_infor='')
    except Exception as e:
        print(e)
        insert_finance_shell_daily(daily_status='error', daily_start_date=daily_start_date,
                                   daily_end_date=daily_end_date,
                                   unusual_point=unusual_id, daily_source='sql', operate_desc='', unusual_infor=str(e))


def mk_utf8resp(js):
    '''
    传入一个字典，返回一个json格式的http回复。
    '''
    resp = make_response(json.dumps(js, ensure_ascii=False))
    resp.headers['Content-Type'] = 'application/json'
    return resp


# http://10.5.138.11:8004/report/query/commoditynames
@report_bp.route('/query/commoditynames', methods=['GET', 'POST'])
def query_commoditynames():
    """
    查询办公费和车辆使用费的大类名称
    :return:
    """
    log.info('---- query_commoditynames ----')

    unusual_id = request.form.get('unusual_id') if request.form.get('unusual_id') else None

    if unusual_id is None:
        data = {"result": "error", "details": "输入的 unusual_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if unusual_id not in ['26', '42', '55']:
        data = {"result": "error", "details": "只能查询检查点26,42或55的大类", "code": 500, 'unusual_id': unusual_id}
        response = jsonify(data)
        return response

    type_str = None
    try:
        if unusual_id == '42':
            type_str = '办公费'
            data = query_checkpoint_55_commoditynames()
        elif unusual_id == '55':
            type_str = '车辆使用费'
            data = query_checkpoint_42_commoditynames()
        elif unusual_id == '26':
            type_str = '会议费'
            data = query_checkpoint_26_commoditynames()
        result = {
            'type': type_str,
            'data': data,
            'unusual_id': unusual_id
        }
    except Exception as e:
        print(e)
        result = {
            'status': 'error',
            'desc': str(e),
            'unusual_id': unusual_id
        }

    # response = jsonify(result)
    # return response, 200
    return mk_utf8resp(result)


# http://10.5.138.11:8004/report/query/product/keyswords
@report_bp.route('/query/product/keyswords', methods=['POST', 'GET'])
def query_product_keywords():
    """
    查询办公费和车辆使用费的商品关键字
    :return:
    """
    log.info('---- query_productnames ----')

    unusual_id = request.form.get('unusual_id') if request.form.get('unusual_id') else None

    if unusual_id is None:
        data = {"result": "error", "details": "输入的 unusual_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if unusual_id not in ['26 ', '42', '55']:
        data = {"result": "error", "details": "只能查询检查点26,42或55的大类", "code": 500, 'unusual_id': unusual_id}
        response = jsonify(data)
        return response

    type_str, keywords = None, None

    try:
        if unusual_id == '42':
            type_str = '办公费'
            keywords = get_office_bill_jiebaword()
        elif unusual_id == '55':
            type_str = '车辆使用费'
            keywords = get_car_bill_jiebaword()
        elif unusual_id == '26':
            type_str = '会议费'
            keywords = get_conference_bill_jiebaword()
        result = {
            'type': type_str,
            'keywords': keywords,
            'unusual_id': unusual_id
        }
    except Exception as e:
        print(e)
        result = {
            'status': 'error',
            'desc': str(e),
            'unusual_id': unusual_id
        }

    return mk_utf8resp(result)


# http://10.5.138.11:8004/report/set/category/sign
@report_bp.route('/set/category/sign', methods=['POST', 'GET'])
def set_finance_category_sign():
    """
    设置办公费和车辆使用费的大类的状态
    :return:
    """
    log.info('---- set_finance_category_sign ----')

    unusual_id = request.form.get('unusual_id') if request.form.get('unusual_id') else None
    category_classify = request.form.get('category_classify') if request.form.get('category_classify') else None

    if unusual_id is None:
        data = {"result": "error", "details": "输入的 unusual_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if unusual_id not in ['26', '42', '55']:
        data = {"result": "error", "details": "只能查询检查点26,42或55的大类", "code": 500, 'unusual_id': unusual_id}
        response = jsonify(data)
        return response

    if category_classify is None:
        data = {"result": "error", "details": "输入的 category_classify 不能为空", "code": 500}
        response = jsonify(data)
        return response

    category_names = request.form.getlist("category_names")

    try:
        update_finance_category_sign(unusual_id, category_names, category_classify)

        if unusual_id == '42':
            type_str = '办公费'
        elif unusual_id == '55':
            type_str = '车辆使用费'
        elif unusual_id == '26':
            type_str = '会议费'

        result = {
            'status': 'ok',
            'desc': f'成功修改检查点{unusual_id}选中的大类状态',
            'unusual_id': unusual_id,
            'type': type_str
        }
    except Exception as e:
        print(e)
        result = {
            'status': 'error',
            'desc': str(e),
            'unusual_id': unusual_id
        }

    return mk_utf8resp(result)


# http://10.5.138.11:8004/report/query/category/sign
@report_bp.route('/query/category/sign', methods=['POST', 'GET'])
def query_finance_category_signs():
    """
    设置办公费和车辆使用费的大类的状态
    :return:
    """
    log.info('---- query_finance_category_sign ----')

    unusual_id = request.form.get('unusual_id') if request.form.get('unusual_id') else None
    category_classify = request.form.get('category_classify') if request.form.get('category_classify') else None

    if unusual_id is None:
        data = {"result": "error", "details": "输入的 unusual_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if unusual_id not in ['26', '42', '55']:
        data = {"result": "error", "details": "只能查询检查点42或55的大类", "code": 500, 'unusual_id': unusual_id}
        response = jsonify(data)
        return response

    if category_classify is None:
        data = {"result": "error", "details": "输入的 category_classify 不能为空", "code": 500}
        response = jsonify(data)
        return response

    try:
        # category_classify 类别, 1 代表商品大类，2 代表商品关键字
        checked_data = query_finance_category_sign(unusual_id, category_classify)

        type_str, records = None, None
        if unusual_id == '42' and category_classify == '1':
            # 商品大类
            type_str = '办公费'
            records = query_checkpoint_55_commoditynames()
        elif unusual_id == '55' and category_classify == '1':
            # 商品大类
            type_str = '车辆使用费'
            records = query_checkpoint_42_commoditynames()
        elif unusual_id == '26' and category_classify == '1':
            # 商品大类
            type_str = '会议费'
            records = query_checkpoint_26_commoditynames()
        elif unusual_id == '42' and category_classify == '2':
            # 商品关键字
            type_str = '办公费'
            records = get_office_bill_jiebaword()
        elif unusual_id == '55' and category_classify == '2':
            # 商品关键字
            type_str = '车辆使用费'
            records = get_car_bill_jiebaword()
        elif unusual_id == '26' and category_classify == '2':
            # 商品关键字
            type_str = '会议费'
            records = get_conference_bill_jiebaword()

        checked_record_ls = []
        for record in records:
            temp = {}
            temp['name'] = record
            temp['status'] = 0

            for checked_record in checked_data:
                if record == checked_record:
                    temp['status'] = 1
                    break

            checked_record_ls.append(temp)

            # print(record)

        result = {
            'status': 'ok',
            'category_classify': category_classify,
            'unusual_id': unusual_id,
            'type': type_str,
            'data': checked_record_ls
        }
    except Exception as e:
        print(e)
        result = {
            'status': 'error',
            'desc': str(e),
            'unusual_id': unusual_id
        }

    return mk_utf8resp(result)


# http://10.5.138.11:8004/report/check/scope
@report_bp.route('/check/scope', methods=['POST', 'GET'])
def check_scope():
    log.info('---- check_scope ----')

    unusual_id = str(request.form.get('unusual_id')) if request.form.get('unusual_id') else None
    current_page = int(request.form.get('current_page')) if request.form.get('current_page') else None

    if current_page is None:
        data = {"result": "error", "details": "输入的 current_page 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if unusual_id is None:
        data = {"result": "error", "details": "输入的 unusual_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if unusual_id not in ['26', '42', '55']:
        data = {"result": "error", "details": "只能查询检查点 26,42或55的大类", "code": 500, 'unusual_id': unusual_id}
        response = jsonify(data)
        return response

    # 商品大类
    category_names = request.form.getlist("category_names")
    # 商品关键字
    good_keywords = request.form.getlist("good_keywords")

    log.info(unusual_id)
    log.info(category_names)
    log.info(good_keywords)

    try:
        if unusual_id == '26':
            count_records, sql, columns_ls = pagination_conference_records(categorys=category_names,
                                                                           good_keywords=good_keywords)
            # log.info(count_records, sql, columns_ls)

            page_obj = Pagination(current_page=current_page, all_count=count_records, per_page_num=10)
            records = page_obj.exec_sql(sql, columns_ls)
        elif unusual_id == '42':
            count_records, sql, columns_ls = pagination_office_records(categorys=category_names,
                                                                       good_keywords=good_keywords)
            # log.info(count_records, sql, columns_ls)
            page_obj = Pagination(current_page=current_page, all_count=count_records, per_page_num=10)
            records = page_obj.exec_sql(sql, columns_ls)

        elif unusual_id == '55':
            count_records, sql, columns_ls = pagination_car_records(categorys=category_names,
                                                                    good_keywords=good_keywords)
            # print(count_records, sql, columns_ls)
            page_obj = Pagination(current_page=current_page, all_count=count_records, per_page_num=10)
            records = page_obj.exec_sql(sql, columns_ls)

        result = {
            'status': 'ok',
            'current_page': current_page,
            'all_count': count_records,
            'records': records
        }

    except Exception as e:
        print(e)
        result = {
            'status': 'error',
            'desc': str(e),
            'unusual_id': unusual_id
        }

    return mk_utf8resp(result)
