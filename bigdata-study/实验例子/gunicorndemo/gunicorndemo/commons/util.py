# -*- coding: utf-8 -*-

"""
Created on

@author:
"""


from datetime import datetime
import time
from decimal import Decimal
import os


def get_current_time():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def convert_digital_precision(val, precision=2):
    """ 保留小数点以后6位 """

    decimal = '0.'
    for i in range(precision):
        decimal += '0'

    if val is None:
        val = 0

    return Decimal(val).quantize(Decimal(decimal))
