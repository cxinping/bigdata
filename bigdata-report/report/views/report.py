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


































