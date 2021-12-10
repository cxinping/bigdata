# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request

from report.commons.connect_kudu2 import prod_execute_sql

from report.commons.logging import get_logger
from report.commons.tools import transfer_content
from report.commons.settings import CONN_TYPE

log = get_logger(__name__)
test_bp = Blueprint('test', __name__)

# http://10.5.138.11:8004/test/finance_unusual/update
@test_bp.route('/finance_unusual/update', methods=['POST'])
def finance_unusual_update():
    log.info('----- test finance_unusual update -----')
    unusual_id = request.form.get('unusual_id') if request.form.get('unusual_id') else None
    unusual_shell = request.form.get('unusual_shell') if request.form.get('unusual_shell') else None

    log.info(f'unusual_id={unusual_id}')
    #log.info(f'unusual_shell={unusual_shell}')

    unusual_shell = transfer_content(unusual_shell)
    log.info(unusual_shell)

    if unusual_id is None:
        data = {"result": "error", "details": "输入的 unusual_id 不能为空", "code": 500}
        response = jsonify(data)
        return response

    sql = f"""
    update 01_datamart_layer_007_h_cw_df.finance_unusual set unusual_shell="{unusual_shell}"
    where unusual_id='{unusual_id}'
    """

    log.info(sql)

    try:
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)

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