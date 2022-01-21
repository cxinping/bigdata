# -*- coding: utf-8 -*-
import datetime
import dateutil.relativedelta


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


def get_cal_date(n, fmt='%Y%m'):
    """
    获得经过计算的时间，当n为正数是，表示查询下几月的时间。当n为负数时，表示计算前几个月的时间
    :return:
    """
    now = datetime.datetime.now()
    date = now + dateutil.relativedelta.relativedelta(months=n)

    return date.strftime(fmt)
