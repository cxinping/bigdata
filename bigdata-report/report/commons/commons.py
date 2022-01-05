# -*- coding: utf-8 -*-
import datetime

def get_date_month(n=0):
    """
    :param n: 获取当前时间n月之前的时间
    :return: YYYY-MM
    """
    date = datetime.datetime.today()
    month = date.month
    year = date.year
    for i in range(n):
        if month == 1:
            year -= 1
            month = 12
        else:
            month -= 1
    return datetime.date(year, month, 1).strftime('%Y%m')