# -*- coding: utf-8 -*-

'''
Created on 2021-08-02

@author: WangShuo
'''

from flask import Blueprint, jsonify, render_template, request
from flask import current_app as app
from http import HTTPStatus
import datetime
import time
from report.commons.logging import get_logger

log = get_logger(__name__)
report_bp = Blueprint('report', __name__)


@report_bp.route('/test/<data>', methods=['GET'])
def test_report(data):
    log.info('---- test ============')

    result = {
        'name': 'wang',
        'time': datetime.datetime.now()
    }

    response = jsonify(result)
    return response, 200

















