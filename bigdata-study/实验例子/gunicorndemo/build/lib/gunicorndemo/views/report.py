# -*- coding: utf-8 -*-

"""
Created on 2020-12-03

@author: Wang Shuo
"""

from flask import Blueprint, jsonify
from flask import render_template
import time

from ..commons.logging import log_entry_exit, get_logger
from ..exts import db

report_bp = Blueprint('report', __name__)

log = get_logger()


@report_bp.route('/init/db', methods=['GET'])
def init_db():
    db.drop_all(bind=None)
    db.create_all(bind=None)

    return "init LRE db "

# http://127.0.0.1:8888/report/hello
@report_bp.route('/hello')
@report_bp.route('/hello/<name>')
def hello(name=None):
    return render_template('index.html', name=name)

# http://127.0.0.1:8888/report/hello2
# http://192.168.11.10:8888/report/hello2
@report_bp.route("/hello2")
def hello2():
    return "<h1 style='color:blue;text-align: center;'>Hello world!</h1>"





