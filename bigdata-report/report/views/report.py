# -*- coding: utf-8 -*-

'''
Created on 2021-08-02

@author: WangShuo
'''

from flask import Blueprint, jsonify, render_template, request
from flask import current_app as app
from http import HTTPStatus
import datetime,time
import json
from report.commons.logging import get_logger
from report.commons.connect_kudu import prod_execute_sql


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
        'gender' : gender if gender else '',
        'code' : '001',
        'data' : data_ls
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
        'gender' : gender if gender else '',
        'code' : '001',
        'data' : data_ls
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
            'details': '成功新增一条费用标准（finance_standard）'
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
            'details': '成功修改一条费用标准（finance_standard）'
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
            'details': '成功删除一条费用标准（finance_standard）'
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
            'details': '成功新增一条风景名胜记录'
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
            'details': '成功修改一条风景名胜记录'
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

    sql = f"""
    insert into 01_datamart_layer_007_h_cw_df.finance_person(person_id, company_code, inner_code, sum_person, pesiod )
    values('{person_id}','{company_code}','{inner_code}', {sum_person}, '{pesiod}' )
        """.replace('\n', '')

    print(sql)
    try:
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
            'details': '成功修改一条"集团和股份公司人数"记录'
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


















