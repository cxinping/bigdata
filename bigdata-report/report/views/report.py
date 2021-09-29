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
    standard_value = request.form.get('standard_value') if request.form.get('standard_value') else None
    out_value = request.form.get('out_value') if request.form.get('out_value') else None

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
    values('{standard_id}','{unusual_id}','{unusual_level}','{standard_value}','{out_value}') 
    """.replace('\n', '')

    print(sql)
    try:
        #prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)

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
    standard_value = request.form.get('standard_value') if request.form.get('standard_value') else None
    out_value = request.form.get('out_value') if request.form.get('out_value') else None

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
    values('{standard_id}','{unusual_id}','{unusual_level}','{standard_value}','{out_value}') 
    """.replace('\n', '')

    print(sql)
    try:
        #prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)

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
        #prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)

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

























