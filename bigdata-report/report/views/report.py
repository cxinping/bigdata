# -*- coding: utf-8 -*-

"""
Created on 2021-08-02

@author: WangShuo
"""

from concurrent.futures import ThreadPoolExecutor
import datetime
import json
from flask import Blueprint, jsonify, request, make_response
import traceback
import time
from report.commons.connect_kudu2 import prod_execute_sql
from report.commons.logging import get_logger
from report.commons.tools import transfer_content
from report.services.office_expenses_service import (query_checkpoint_42_commoditynames, get_office_bill_jiebaword,
                                                     pagination_office_records)
from report.services.vehicle_expense_service import (query_checkpoint_55_commoditynames, get_car_bill_jiebaword,
                                                     pagination_car_records)
from report.services.conference_expense_service import (pagination_conference_records, get_conference_bill_jiebaword,
                                                        query_checkpoint_26_commoditynames)
from report.commons.tools import get_current_time
from report.services.common_services import (insert_finance_shell_daily, update_finance_shell_daily,
                                             query_finance_shell_daily_status,
                                             operate_finance_category_sign, clean_finance_category_sign,
                                             query_finance_category_signs,
                                             query_finance_category_sign, pagination_finance_shell_daily_records)
from report.services.temp_api_bill_services import (exec_temp_api_bill_sql_by_target, exec_temp_api_bill_sql_by_ids,
                                                    insert_temp_api_bill, update_temp_api_bill,
                                                    delete_temp_api_bill, pagination_temp_api_bill_records)
from report.commons.db_helper import Pagination
from report.commons.runengine import (execute_task, execute_py_shell, execute_kudu_sql)
from report.services.travel_expense_service import get_travel_keyword
from report.services.data_process_services import (insert_temp_performance_bill, update_temp_performance_bill,
                                                   del_temp_performance_bill, query_temp_performance_bill,
                                                   pagination_temp_performance_bill_records, exec_temp_performance_bill,
                                                   query_finance_data_process)
from report.services.finance_company_code_services import (insert_finance_company_code, update_finance_company_code ,del_finance_company_code, pagination_finance_company_code_records)
from report.commons.settings import CONN_TYPE

log = get_logger(__name__)

report_bp = Blueprint('report', __name__)

executor = ThreadPoolExecutor(200)

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
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
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
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)

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
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)

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
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
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
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
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
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
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
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)

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
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
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
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
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
    #unusual_type = request.form.get('unusual_type') if request.form.get('unusual_type') else None
    unusual_point = request.form.get('unusual_point') if request.form.get('unusual_point') else None
    unusual_content = request.form.get('unusual_content') if request.form.get('unusual_content') else None
    unusual_shell = request.form.get('unusual_shell') if request.form.get('unusual_shell') else None
    isalgorithm = request.form.get('isalgorithm') if request.form.get('isalgorithm') else None
    sign_status = str(request.form.get('sign_status')) if request.form.get('sign_status') else None
    unusual_level = str(request.form.get('unusual_level')) if request.form.get('unusual_level') else None

    log.info(f'unusual_id={unusual_id}')
    log.info(f'cost_project={cost_project}')
    log.info(f'unusual_number={unusual_number}')
    log.info(f'number_name={number_name}')
    #log.info(f'unusual_type={unusual_type}')
    log.info(f'unusual_point={unusual_point}')
    log.info(f'unusual_content={unusual_content}')
    log.info(f'unusual_shell={unusual_shell}')
    log.info(f'isalgorithm={isalgorithm}')
    log.info(f'sign_status={sign_status}')
    log.info(f'unusual_level={unusual_level}')

    if unusual_id is None:
        data = {"result": "error", "details": "输入的 unusual_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    # if unusual_level is None:
    #     data = {"result": "error", "details": "输入的 unusual_level 不能为空", "code": 500}
    #     response = jsonify(data)
    #     return response


    # 对输入的python脚本进行转义
    unusual_shell = transfer_content(unusual_shell)

    sql = f"""
insert into 01_datamart_layer_007_h_cw_df.finance_unusual(unusual_id ,cost_project, unusual_number, number_name, unusual_point, unusual_content, unusual_shell, isalgorithm,sign_status, unusual_level)
values('{unusual_id}','{cost_project}','{unusual_number}','{number_name}' , '{unusual_point}' , '{unusual_content}', '{unusual_shell}', '{isalgorithm}', '{sign_status}', '{unusual_level}')
    """.replace('\n', '')

    print(sql)

    try:
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)

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
    # 1为sql类, 2为算法类
    isalgorithm = request.form.get('isalgorithm') if request.form.get('isalgorithm') else None
    # 是否执行，1为执行，0为不执行
    sign_status = str(request.form.get('sign_status')) if request.form.get('sign_status') else None
    unusual_level = str(request.form.get('unusual_level')) if request.form.get('unusual_level') else None

    log.info(f'unusual_id={unusual_id}')
    log.info(f'unusual_point={unusual_point}')
    log.info(f'unusual_content={unusual_content}')
    log.info(f'isalgorithm={isalgorithm}')
    log.info(f'sign_status={sign_status}')
    log.info(f'unusual_level={unusual_level}')

    unusual_shell = transfer_content(unusual_shell)
    # log.info(f'* unusual_shell=\n{unusual_shell}')

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

    # if unusual_level is None:
    #     data = {"result": "error", "details": "输入的 unusual_level 不能为空", "code": 500}
    #     response = jsonify(data)
    #     return response


    if isalgorithm is None or isalgorithm not in ['1', '2']:
        data = {"result": "error", "details": "输入的 isalgorithm 不能为空或者isalgorithm不等于'1'或'2'。当isalgorithm等于1为sql类, 2为算法类",
                "code": 500}
        response = jsonify(data)
        return response

    if sign_status is None:
        data = {"result": "error", "details": "输入的 sign_status 不能为空", "code": 500}
        response = jsonify(data)
        return response

    sql = f"""
    update 01_datamart_layer_007_h_cw_df.finance_unusual set unusual_point='{unusual_point}', unusual_content='{unusual_content}', unusual_shell='{unusual_shell}', isalgorithm="{isalgorithm}" ,
    sign_status='{sign_status}', unusual_level='{unusual_level}'
    where unusual_id='{unusual_id}'
    """

    log.info(sql)

    try:
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)

        data = {
            'result': 'ok',
            'code': 200,
            'details': f"成功修改'检查点{unusual_id}'相关记录"
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
            """#.replace('\n', '')
        print(sql)
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)

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





# http://10.5.138.11:8004/report/finance_unusual/execute
@report_bp.route('/finance_unusual/execute', methods=['POST'])
def finance_unusual_execute():
    log.info('----- finance_unusual execute -----')
    unusual_id = request.form.get('unusual_id') if request.form.get('unusual_id') else None
    log.info(f'unusual_id={unusual_id},{type(unusual_id)}')

    if unusual_id is None:
        log.info('*** unusual_id is None')
        data = {"result": "error", "details": "输入的 unusual_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    try:
        sql = f"""
            select unusual_id,unusual_shell,isalgorithm from 01_datamart_layer_007_h_cw_df.finance_unusual where unusual_id='{unusual_id}'
                """.replace('\n', '')

        # log.info(sql)

        result = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
        unusual_shell = str(result[0][1])
        # "1为sql类, 2为算法类"
        isalgorithm = str(result[0][2])

        if unusual_shell is None:
            data = {"result": "error", "details": f"查询的 {unusual_id} 对应的 unusual_shell 不能为空", "code": 500}
            response = jsonify(data)
            return response

        if isalgorithm is None:
            data = {"result": "error", "details": f"查询的 {unusual_id} 对应的 isalgorithm 不能为空", "code": 500}
            response = jsonify(data)
            return response

        record = query_finance_shell_daily_status(unusual_point=unusual_id, task_status='doing')
        if record:
            task_str = None
            if isalgorithm == '1':
                task_str = 'SQL'
            elif isalgorithm == '2':
                task_str = 'Python Shell脚本'
            data = {"result": "error", "details": f"正在运行检查点{unusual_id}的{task_str}，请稍后执行", "code": 500}
            response = jsonify(data)
            return response

        if isalgorithm == '1':
            ######### 执行 SQL ############
            executor.submit(execute_kudu_sql, unusual_shell, unusual_id)
            # execute_kudu_sql(unusual_shell, unusual_id)

        elif isalgorithm == '2':
            ###### 执行算法 python 脚本  ############
            executor.submit(execute_py_shell, unusual_shell, unusual_id)
            # execute_py_shell(unusual_shell, unusual_id)

        # execute_task(isalgorithm=isalgorithm,unusual_shell=unusual_shell, unusual_id=unusual_id)

        daily_source = 'SQL' if isalgorithm == '1' else 'Python Shell'
        data = {
            'result': 'ok',
            'code': 200,
            'details': f'正在执行检查点{unusual_id}的{daily_source}'
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

    if unusual_id not in ['26', '42', '55', '16']:
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
        elif unusual_id == '16':
            type_str = '差旅费'
            data = get_travel_keyword()

        result = {
            'type': type_str,
            'data': data,
            'total': len(data),
            'unusual_id': unusual_id
        }
        return mk_utf8resp(result)
    except Exception as e:
        print(e)
        result = {
            'status': 'error',
            'desc': str(e),
            'unusual_id': unusual_id
        }

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
    # print(f' unusual_id={unusual_id}', type(unusual_id))

    if unusual_id is None:
        data = {"result": "error", "details": "输入的 unusual_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if unusual_id not in ['26', '42', '55']:
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
            'total': len(keywords),
            'unusual_id': unusual_id
        }
        return mk_utf8resp(result)
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
    # 类别, 区分大类和商品关键字， 01代表大类，02代表商品关键字
    category_classify = request.form.get('category_classify') if request.form.get('category_classify') else None
    category_names = request.form.get('category_names') if request.form.get('category_names') else None

    # category_names = request.form.getlist("category_names")

    if unusual_id is None:
        data = {"result": "error", "details": "输入的 unusual_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if category_classify is None:
        data = {"result": "error", "details": "输入的 category_classify 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if category_names is None or len(category_names) == 0:
        data = {"result": "error", "details": "输入的 category_names 不能为空 或者 没有传递值", "code": 500}
        response = jsonify(data)
        return response

    if unusual_id not in ['16', '26', '42', '55']:
        data = {"result": "error", "details": "只能查询检查点 16, 26 ,42 或55 的大类", "code": 500, 'unusual_id': unusual_id}
        response = jsonify(data)
        return response

    if category_classify is None:
        data = {"result": "error", "details": "输入的 category_classify 不能为空", "code": 500}
        response = jsonify(data)
        return response

    try:
        type_str = None
        available_category_name = None  # 存在的的商品关键字或商品大类

        # category_classify 类别, 区分大类和商品关键字， 01代表大类，02代表商品关键字
        if unusual_id == '42' and category_classify == '01':
            type_str = '办公费'
            available_category_name = query_checkpoint_42_commoditynames()
        elif unusual_id == '42' and category_classify == '02':
            type_str = '办公费'
            available_category_name = get_office_bill_jiebaword()
        elif unusual_id == '55' and category_classify == '01':
            type_str = '车辆使用费'
            available_category_name = query_checkpoint_55_commoditynames()
        elif unusual_id == '55' and category_classify == '02':
            type_str = '车辆使用费'
            available_category_name = get_car_bill_jiebaword()
        elif unusual_id == '26' and category_classify == '01':
            type_str = '会议费'
            available_category_name = query_checkpoint_26_commoditynames()
        elif unusual_id == '26' and category_classify == '02':
            type_str = '会议费'
            available_category_name = get_conference_bill_jiebaword()
        elif unusual_id == '16' and category_classify == '02':
            type_str = '差旅费'
            available_category_name = get_travel_keyword()

        category_names = str(category_names).split(',')

        print(f'* unusual_id={unusual_id}, category_classify={category_classify} ')
        print('* available_category_name => ', available_category_name)
        print('* checked category_names => ', category_names)

        for category_name_item in available_category_name[:]:
            for checked_category_name in category_names:
                if category_name_item == checked_category_name:
                    available_category_name.remove(checked_category_name)
                    break

        print('* filter available_category_name => ', available_category_name)

        clean_finance_category_sign(unusual_id)
        operate_finance_category_sign(unusual_id=unusual_id, category_names=category_names,
                                      category_classify=category_classify, sign_status='1')
        operate_finance_category_sign(unusual_id=unusual_id, category_names=available_category_name,
                                      category_classify=category_classify, sign_status='0')

        result = {
            'status': 'ok',
            'desc': f'成功修改检查点{unusual_id}选中的大类状态',
            'unusual_id': unusual_id,
            'type': type_str
        }
        return mk_utf8resp(result)
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
def query_all_finance_category_sign():
    """
    查询办公费和车辆使用费的大类和商品关键字的状态
    :return:
    """
    log.info('---- query_finance_category_sign ----')

    unusual_id = request.form.get('unusual_id') if request.form.get('unusual_id') else None
    # category_classify 类别, 01 代表商品大类，02 代表商品关键字
    category_classify = request.form.get('category_classify') if request.form.get('category_classify') else None

    if unusual_id is None:
        data = {"result": "error", "details": "输入的 unusual_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if unusual_id not in ['16', '26', '42', '55']:
        data = {"result": "error", "details": "只能查询检查点16,26,42或55的大类", "code": 500, 'unusual_id': unusual_id}
        response = jsonify(data)
        return response

    if category_classify is None:
        data = {"result": "error", "details": "输入的 category_classify 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if category_classify not in ['01', '02']:
        data = {"result": "error", "details": "输入的 category_classify 只能为01或02", "code": 500}
        response = jsonify(data)
        return response


    try:
        log.info(f'unusual_id={unusual_id}, category_classify={category_classify}')

        # checked_data = query_finance_category_sign(unusual_id, category_classify)
        #
        type_str, records = None, None
        if unusual_id == '42' and category_classify == '01':
            # 商品大类
            type_str = '办公费'
            # records = query_checkpoint_42_commoditynames()
        elif unusual_id == '55' and category_classify == '01':
            # 商品大类
            type_str = '车辆使用费'
            # records = query_checkpoint_55_commoditynames()
        elif unusual_id == '26' and category_classify == '01':
            # 商品大类
            type_str = '会议费'
            # records = query_checkpoint_26_commoditynames()
        elif unusual_id == '42' and category_classify == '02':
            # 商品关键字
            type_str = '办公费'
            # records = get_office_bill_jiebaword()
        elif unusual_id == '55' and category_classify == '02':
            # 商品关键字
            type_str = '车辆使用费'
            # records = get_car_bill_jiebaword()
        elif unusual_id == '26' and category_classify == '02':
            # 商品关键字
            type_str = '会议费'
            # records = get_conference_bill_jiebaword()
        elif unusual_id == '16' and category_classify == '02':
            # 商品关键字
            type_str = '差旅费'

        checked_datas = query_finance_category_signs(unusual_id, category_classify)
        # log.info(checked_datas)

        # checked_record_ls = []
        # for record in records:
        #     temp = {}
        #     temp['name'] = record
        #     temp['status'] = 0
        #
        #     for checked_record in checked_data:
        #         if record == checked_record:
        #             temp['status'] = 1
        #             break
        #
        #     checked_record_ls.append(temp)

        # print(record)

        result = {
            'status': 'ok',
            'category_classify': category_classify,
            'unusual_id': unusual_id,
            'type': type_str,
            'data': checked_datas
        }
        return mk_utf8resp(result)
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
    page_size = int(request.form.get('page_size')) if request.form.get('page_size') else None

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

    if page_size is None:
        data = {"result": "error", "details": "输入的 page_size 不能为空", "code": 500}
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

            page_obj = Pagination(current_page=current_page, all_count=count_records, per_page_num=page_size)
            records = page_obj.exec_sql(sql, columns_ls)
        elif unusual_id == '42':
            count_records, sql, columns_ls = pagination_office_records(categorys=category_names,
                                                                       good_keywords=good_keywords)
            # log.info(count_records, sql, columns_ls)
            page_obj = Pagination(current_page=current_page, all_count=count_records, per_page_num=page_size)
            records = page_obj.exec_sql(sql, columns_ls)

        elif unusual_id == '55':
            count_records, sql, columns_ls = pagination_car_records(categorys=category_names,
                                                                    good_keywords=good_keywords)
            # print(count_records, sql, columns_ls)
            page_obj = Pagination(current_page=current_page, all_count=count_records, per_page_num=page_size)
            records = page_obj.exec_sql(sql, columns_ls)

        result = {
            'status': 'ok',
            'current_page': current_page,
            'total': count_records,
            'data': records,
            'code': 200
        }
        return mk_utf8resp(result)
    except Exception as e:
        print(e)
        result = {
            'status': 'error',
            'desc': str(e),
            'unusual_id': unusual_id,
            'code': 500
        }
        return mk_utf8resp(result)


# http://10.5.138.11:8004/report/finance_shell_daily/query
@report_bp.route('/finance_shell_daily/query', methods=['POST', 'GET'])
def query_finance_shell_daily():
    log.info('---- query_finance_shell_daily ----')

    unusual_point = str(request.form.get('unusual_point')) if request.form.get('unusual_point') else None
    current_page = int(request.form.get('current_page')) if request.form.get('current_page') else None
    page_size = int(request.form.get('page_size')) if request.form.get('page_size') else None
    daily_type = str(request.form.get('daily_type')) if request.form.get('daily_type') else None

    if unusual_point is None or len(unusual_point) == 0:
        unusual_point = None

    log.info(f'* current_page={current_page},page_size={page_size}, unusual_point => {unusual_point} ')
    log.info(f'daily_type => {daily_type}')

    if current_page is None:
        data = {"result": "error", "details": "输入的 current_page 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if page_size is None:
        data = {"result": "error", "details": "输入的 page_size 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if daily_type is None:
        data = {"result": "error", "details": "输入的 daily_type 不能为空", "code": 500}
        response = jsonify(data)
        return response

    try:
        count_records, sql, columns_ls = pagination_finance_shell_daily_records(unusual_point=unusual_point,
                                                                                daily_type=daily_type)

        # print('count_records => ', count_records)
        # print('sql => ', sql)
        # print('columns_ls => ', columns_ls)

        page_obj = Pagination(current_page=current_page, all_count=count_records, per_page_num=page_size)
        records = page_obj.exec_sql(sql, columns_ls)

        # print('records => ', records)

        result = {
            'result': 'ok',
            'current_page': current_page,
            'total': count_records,
            'data': records,
            'code': 200
        }

        # print('==== show infos =========')
        # print(result)

        return mk_utf8resp(result)
    except Exception as e:
        print(e)
        result = {
            'result': 'error',
            'details': str(e),
            'unusual_point': unusual_point,
            'code': 500
        }

        return mk_utf8resp(result)


############  【临时表相关】  ############

# http://10.5.138.11:8004/report/temp/api/execute/target
@report_bp.route('/temp/api/execute/target', methods=['POST', 'GET'])
def temp_api_execute_by_target():
    log.info('---- temp_api_execute_by_target ----')
    target_classify = str(request.form.get('target_classify')) if request.form.get('target_classify') else None
    #log.info(target_classify)

    if target_classify is None:
        data = {"result": "error", "details": "输入的 target_classify 不能为空", "code": 500}
        response = jsonify(data)
        return response

    try:
        is_log = True
        executor.submit(exec_temp_api_bill_sql_by_target, target_classify, is_log)

        result = {
            'result': 'ok',
            'details': f'正在执行类型为{target_classify}的临时表SQL',
            "code": 200
        }

        return mk_utf8resp(result)
    except Exception as e:
        print(e)
        result = {
            'result': 'error',
            'details': str(e),
            'code': 500
        }
        return mk_utf8resp(result)


# http://10.5.138.11:8004/report/temp/api/execute/ids
@report_bp.route('/temp/api/execute/ids', methods=['POST', 'GET'])
def temp_api_execute_by_ids():
    import time
    log.info('---- temp_api_execute_by_ids ----')
    tem_api_ids = request.form.get('tem_api_ids') if request.form.get('tem_api_ids') else None

    tem_api_ids = str(tem_api_ids).split(',')

    if tem_api_ids is None or len(tem_api_ids) == 0:
        data = {"result": "error", "details": "输入的 tem_api_ids 不能为空 或者 没有传递值", "code": 500}
        response = jsonify(data)
        return response

    try:
        executor.submit(exec_temp_api_bill_sql_by_ids, tem_api_ids)

        result = {
            'result': 'ok',
            'details': f'成功执行了 {len(tem_api_ids)} 条的临时表SQL',
            "code": 200
        }

        return mk_utf8resp(result)
    except Exception as e:
        print(e)
        result = {
            'result': 'error',
            'details': str(e),
            'code': 500
        }
        return mk_utf8resp(result)


# http://10.5.138.11:8004/report/temp/api/add
@report_bp.route('/temp/api/add', methods=['POST'])
def temp_api_add():
    log.info('---- temp_api_add ----')
    order_number = request.form.get('order_number') if request.form.get('order_number') else None
    target_classify = request.form.get('target_classify') if request.form.get('target_classify') else None
    api_sql = request.form.get('api_sql') if request.form.get('api_sql') else None

    if order_number is None:
        data = {"result": "error", "details": "输入的 order_number 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if target_classify is None:
        data = {"result": "error", "details": "输入的 target_classify 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if api_sql is None:
        data = {"result": "error", "details": "输入的 api_sql 不能为空", "code": 500}
        response = jsonify(data)
        return response

    api_sql = transfer_content(api_sql)

    try:
        insert_temp_api_bill(order_number=order_number, target_classify=target_classify, api_sql=api_sql)
        data = {
            'result': 'ok',
            'code': 200,
            'details': '成功添加一条"临时表"记录'
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


# http://10.5.138.11:8004/report/temp/api/update
@report_bp.route('/temp/api/update', methods=['POST'])
def temp_api_update():
    log.info('---- temp_api_update ----')
    tem_api_id = request.form.get('tem_api_id') if request.form.get('tem_api_id') else None
    order_number = request.form.get('order_number') if request.form.get('order_number') else None
    target_classify = request.form.get('target_classify') if request.form.get('target_classify') else None
    api_sql = request.form.get('api_sql') if request.form.get('api_sql') else None

    if tem_api_id is None:
        data = {"result": "error", "details": "输入的 tem_api_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if order_number is None:
        data = {"result": "error", "details": "输入的 order_number 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if target_classify is None:
        data = {"result": "error", "details": "输入的 target_classify 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if api_sql is None:
        data = {"result": "error", "details": "输入的 api_sql 不能为空", "code": 500}
        response = jsonify(data)
        return response

    api_sql = transfer_content(api_sql)

    try:
        update_temp_api_bill(tem_api_id=tem_api_id, order_number=order_number, target_classify=target_classify,
                             api_sql=api_sql)
        data = {
            'result': 'ok',
            'code': 200,
            'details': '成功修改一条"临时表"记录'
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


# http://10.5.138.11:8004/report/temp/api/query
@report_bp.route('/temp/api/query', methods=['POST'])
def temp_api_query():
    log.info('---- temp_api_query ----')
    current_page = int(request.form.get('current_page')) if request.form.get('current_page') else None
    page_size = int(request.form.get('page_size')) if request.form.get('page_size') else None

    log.info(f'current_page={current_page},page_size={page_size}')

    if current_page is None:
        data = {"result": "error", "details": "输入的 current_page 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if page_size is None:
        data = {"result": "error", "details": "输入的 page_size 不能为空", "code": 500}
        response = jsonify(data)
        return response

    try:
        count_records, sql, columns_ls = pagination_temp_api_bill_records()
        page_obj = Pagination(current_page=current_page, all_count=count_records, per_page_num=page_size)
        records = page_obj.exec_sql(sql, columns_ls)

        result = {
            'status': 'ok',
            'current_page': current_page,
            'total': count_records,
            'data': records,
            'code': 200
        }
        return mk_utf8resp(result)
    except Exception as e:
        print(e)
        result = {
            'status': 'error',
            'desc': str(e),
            'code': 500
        }
        return mk_utf8resp(result)


# http://10.5.138.11:8004/report/temp/api/delete
@report_bp.route('/temp/api/delete', methods=['POST'])
def temp_api_delete():
    log.info('---- temp_api_delete ----')
    tem_api_ids = request.form.get('tem_api_ids') if request.form.get('tem_api_ids') else None

    # log.info(f'111 tem_api_ids={tem_api_ids}')

    if tem_api_ids is None or len(tem_api_ids) == 0:
        data = {"result": "error", "details": "输入的 tem_api_ids 不能为空 或者 没有传递值", "code": 500}
        response = jsonify(data)
        return response

    try:
        tem_api_ids = str(tem_api_ids).split(',')

        delete_temp_api_bill(tem_api_ids)

        data = {
            'result': 'ok',
            'code': 200,
            'details': f'成功删除{len(tem_api_ids)}条"临时表"记录'
        }
        response = jsonify(data)
        return response
    except Exception as e:
        print(e)
        result = {
            'status': 'error',
            'desc': str(e),
            'code': 500
        }
        return mk_utf8resp(result)


############  【绩效临时表相关】  ############

# http://10.5.138.11:8004/report/temp/performance/bill/add
@report_bp.route('/temp/performance/bill/add', methods=['POST'])
def temp_performance_bill_add():
    log.info('----- temp_performance_bill_add add -----')
    order_number = request.form.get('order_number') if request.form.get('order_number') else None
    describe_num = request.form.get('describe_num') if request.form.get('describe_num') else ''
    sign_status = request.form.get('sign_status') if request.form.get('sign_status') else None
    performance_sql = request.form.get('performance_sql') if request.form.get('performance_sql') else None
    target_classify = request.form.get('target_classify') if request.form.get('target_classify') else None

    # log.info(f'order_number={order_number}')
    # log.info(f'describe_num={describe_num}')
    # log.info(f'sign_status={sign_status}')
    # log.info(f'performance_sql={performance_sql}')

    if order_number is None:
        data = {"result": "error", "details": "输入的 order_number 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if sign_status is None:
        data = {"result": "error", "details": "输入的 sign_status 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if performance_sql is None:
        data = {"result": "error", "details": "输入的 performance_sql 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if target_classify is None:
        data = {"result": "error", "details": "输入的 target_classify 不能为空", "code": 500}
        response = jsonify(data)
        return response

    # 对输入的脚本进行转义
    performance_sql = transfer_content(performance_sql)

    try:
        insert_temp_performance_bill(order_number=order_number, target_classify=target_classify,describe_num=describe_num, sign_status=sign_status,
                                     performance_sql=performance_sql)
        data = {
            'result': 'ok',
            'code': 200,
            'details': '成功新增一条"绩效临时表"记录'
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


# http://10.5.138.11:8004/report/temp/performance/bill/update
@report_bp.route('/temp/performance/bill/update', methods=['POST'])
def temp_performance_bill_update():
    log.info('----- temp_performance_bill_update -----')
    performance_id = request.form.get('performance_id') if request.form.get('performance_id') else None
    order_number = request.form.get('order_number') if request.form.get('order_number') else None
    describe_num = request.form.get('describe_num') if request.form.get('describe_num') else ''
    sign_status = request.form.get('sign_status') if request.form.get('sign_status') else None
    performance_sql = request.form.get('performance_sql') if request.form.get('performance_sql') else None
    target_classify = request.form.get('target_classify') if request.form.get('target_classify') else None

    if performance_id is None:
        data = {"result": "error", "details": "输入的 performance_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if order_number is None:
        data = {"result": "error", "details": "输入的 order_number 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if sign_status is None:
        data = {"result": "error", "details": "输入的 sign_status 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if performance_sql is None:
        data = {"result": "error", "details": "输入的 performance_sql 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if target_classify is None:
        data = {"result": "error", "details": "输入的 target_classify 不能为空", "code": 500}
        response = jsonify(data)
        return response

    # 对输入的脚本进行转义
    performance_sql = transfer_content(performance_sql)

    try:
        update_temp_performance_bill(performance_id=performance_id, order_number=order_number,
                                     target_classify=target_classify,
                                     describe_num=describe_num, sign_status=sign_status,
                                     performance_sql=performance_sql)
        data = {
            'result': 'ok',
            'code': 200,
            'details': '成功修改一条"绩效临时表"记录'
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


# http://10.5.138.11:8004/report/temp/performance/bill/query
@report_bp.route('/temp/performance/bill/query', methods=['POST'])
def temp_performance_bill_query():
    log.info('---- temp_performance_bill_query ----')
    current_page = int(request.form.get('current_page')) if request.form.get('current_page') else None
    page_size = int(request.form.get('page_size')) if request.form.get('page_size') else None

    log.info(f'current_page={current_page},page_size={page_size}')

    if current_page is None:
        data = {"result": "error", "details": "输入的 current_page 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if page_size is None:
        data = {"result": "error", "details": "输入的 page_size 不能为空", "code": 500}
        response = jsonify(data)
        return response

    try:
        count_records, sql, columns_ls = pagination_temp_performance_bill_records()
        page_obj = Pagination(current_page=current_page, all_count=count_records, per_page_num=page_size)
        records = page_obj.exec_sql(sql, columns_ls)

        result = {
            'status': 'ok',
            'current_page': current_page,
            'total': count_records,
            'data': records,
            'code': 200
        }
        return mk_utf8resp(result)
    except Exception as e:
        print(e)
        result = {
            'status': 'error',
            'desc': str(e),
            'code': 500
        }
        return mk_utf8resp(result)


# http://10.5.138.11:8004/report/temp/performance/bill/delete
@report_bp.route('/temp/performance/bill/delete', methods=['POST'])
def temp_performance_bill_delete():
    log.info('---- temp_performance_bill_delete ---- ')
    performance_ids = request.form.get('performance_ids') if request.form.get('performance_ids') else None
    # print(performance_ids)

    if performance_ids is None or len(performance_ids) == 0:
        data = {"result": "error", "details": "输入的 performance_ids 不能为空 或者 没有传递值", "code": 500}
        response = jsonify(data)
        return response

    try:
        performance_ids = str(performance_ids).split(',')
        #print(performance_ids)

        del_temp_performance_bill(performance_ids)

        data = {
            'result': 'ok',
            'code': 200,
            'details': f'成功删除{len(performance_ids)}条"绩效临时表"记录'
        }
        response = jsonify(data)
        return response
    except Exception as e:
        print(e)
        result = {
            'status': 'error',
            'desc': str(e),
            'code': 500
        }
        return mk_utf8resp(result)


# http://10.5.138.11:8004/report/temp/performance/bill/execute
@report_bp.route('/temp/performance/bill/execute', methods=['POST'])
def temp_performance_bill_execute():
    log.info('---- temp_performance_bill_execute ---- ')
    performance_ids = request.form.get('performance_ids') if request.form.get('performance_ids') else None
    # print(performance_ids)

    if performance_ids is None or len(performance_ids) == 0:
        data = {"result": "error", "details": "输入的 performance_ids 不能为空 或者 没有传递值", "code": 500}
        response = jsonify(data)
        return response

    try:
        performance_ids = str(performance_ids).split(',')
        if len(performance_ids) > 0:
            start_time = time.perf_counter()
            executor.submit(exec_temp_performance_bill, performance_ids)
            consumed_time = round(time.perf_counter() - start_time)

        data = {
            'result': 'ok',
            'code': 200,
            'details': f'正在执行{len(performance_ids)}条绩效临时表的SQL',
            "consumed_time": consumed_time
        }
        response = jsonify(data)
        return response

    except Exception as e:
        print(e)
        result = {
            'status': 'error',
            'desc': str(e),
            'code': 500
        }
        return mk_utf8resp(result)


############  【流程表相关】  ############

# http://10.5.138.11:8004/report/finance_data/data/process/query
@report_bp.route('/finance_data/data/process/query', methods=['POST'])
def finance_data_process_query():
    log.info('---- finance_data_process_query ---- ')
    query_date = request.form.get('query_date') if request.form.get('query_date') else None
    #log.info(query_date)

    if query_date is None or len(query_date) == 0:
        data = {"result": "error", "details": "输入的 query_date 不能为空 或者 没有传递值", "code": 500}
        response = jsonify(data)
        return response

    try:
        records = query_finance_data_process(query_date)

        result = {
            'status': 'ok',
            'data': records,
            'code': 200
        }
        return mk_utf8resp(result)
    except Exception as e:
        print(e)
        result = {
            'status': 'error',
            'desc': str(e),
            'code': 500
        }
        return mk_utf8resp(result)

############  【单位code表】  ############


# http://10.5.138.11:8004/report/finance/company/code/query
@report_bp.route('/finance/company/code/query', methods=['POST'])
def finance_company_code_query():
    log.info('---- finance_company_code_query ----')
    current_page = int(request.form.get('current_page')) if request.form.get('current_page') else None
    page_size = int(request.form.get('page_size')) if request.form.get('page_size') else None
    company_name = request.form.get('company_name') if request.form.get('company_name') else None

    log.info(f'current_page={current_page},page_size={page_size},company_name={company_name}')

    if current_page is None:
        data = {"result": "error", "details": "输入的 current_page 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if page_size is None:
        data = {"result": "error", "details": "输入的 page_size 不能为空", "code": 500}
        response = jsonify(data)
        return response

    try:
        count_records, sql, columns_ls = pagination_finance_company_code_records(company_name=company_name)
        page_obj = Pagination(current_page=current_page, all_count=count_records, per_page_num=page_size)
        records = page_obj.exec_sql(sql, columns_ls)

        result = {
            'status': 'ok',
            'current_page': current_page,
            'total': count_records,
            'data': records,
            'code': 200
        }
        return mk_utf8resp(result)
    except Exception as e:
        print(e)
        result = {
            'status': 'error',
            'desc': str(e),
            'code': 500
        }
        return mk_utf8resp(result)


# http://10.5.138.11:8004/report/finance/company/code/add
@report_bp.route('/finance/company/code/add', methods=['POST'])
def finance_company_code_add():
    log.info('----- finance_company_code_add -----')
    company_name = request.form.get('company_name') if request.form.get('company_name') else None
    company_code = request.form.get('company_code') if request.form.get('company_code') else ''
    company_old_code = request.form.get('company_old_code') if request.form.get('company_old_code') else None
    iscompany = request.form.get('iscompany') if request.form.get('iscompany') else None

    # print(f'company_name={company_name}')
    # print(f'company_code={company_code}')
    # print(f'company_old_code={company_old_code}')
    # print(f'iscompany={iscompany}')

    if company_name is None:
        data = {"result": "error", "details": "输入的 company_name 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if company_code is None:
        data = {"result": "error", "details": "输入的 company_code 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if company_old_code is None:
        data = {"result": "error", "details": "输入的 company_old_code 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if iscompany is None:
        data = {"result": "error", "details": "输入的 iscompany 不能为空", "code": 500}
        response = jsonify(data)
        return response
    try:
        insert_finance_company_code(company_name=company_name, company_code=company_code, company_old_code=company_old_code, iscompany=iscompany)
        data = {
            'result': 'ok',
            'code': 200,
            'details': '成功新增一条"单位code表"记录'
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


# http://10.5.138.11:8004/report/finance/company/code/update
@report_bp.route('/finance/company/code/update', methods=['POST'])
def finance_company_code_update():
    log.info('----- finance_company_code_update -----')
    id = request.form.get('id') if request.form.get('id') else None
    company_name = request.form.get('company_name') if request.form.get('company_name') else None
    company_code = request.form.get('company_code') if request.form.get('company_code') else ''
    company_old_code = request.form.get('company_old_code') if request.form.get('company_old_code') else None
    iscompany = request.form.get('iscompany') if request.form.get('iscompany') else None

    if id is None:
        data = {"result": "error", "details": "输入的 id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if company_name is None:
        data = {"result": "error", "details": "输入的 company_name 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if company_code is None:
        data = {"result": "error", "details": "输入的 company_code 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if company_old_code is None:
        data = {"result": "error", "details": "输入的 company_old_code 不能为空", "code": 500}
        response = jsonify(data)
        return response

    if iscompany is None:
        data = {"result": "error", "details": "输入的 iscompany 不能为空", "code": 500}
        response = jsonify(data)
        return response

    try:
        update_finance_company_code(id=id, company_name=company_name, company_code=company_code, company_old_code=company_old_code, iscompany=iscompany)

        data = {
            'result': 'ok',
            'code': 200,
            'details': '成功修改一条"单位code表"记录'
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


# http://10.5.138.11:8004/report/finance/company/code/delete
@report_bp.route('/finance/company/code/delete', methods=['POST'])
def finance_company_code_delete():
    log.info('---- finance_company_code_delete ---- ')
    ids = request.form.get('ids') if request.form.get('ids') else None
    # print(ids)

    if ids is None or len(ids) == 0:
        data = {"result": "error", "details": "输入的 ids 不能为空 或者 没有传递值", "code": 500}
        response = jsonify(data)
        return response

    try:
        ids = str(ids).split(',')
        #print(ids)

        del_finance_company_code(ids)

        data = {
            'result': 'ok',
            'code': 200,
            'details': f'成功删除{len(ids)}条单位code表"记录'
        }
        response = jsonify(data)
        return response
    except Exception as e:
        print(e)
        result = {
            'status': 'error',
            'desc': str(e),
            'code': 500
        }
        return mk_utf8resp(result)

