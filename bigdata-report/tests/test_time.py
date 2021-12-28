# -*- coding: utf-8 -*-
import datetime

# 得到现在的时间
now = datetime.datetime.now()
# YYYY-MM-DD日期格式
date_now = now.date()


def get_date_month(mon=0):
    '''
    :param mon: 获取当前时间X月之前的时间
    :return: YYYY-MM
    '''
    # 当前时间X个月前
    last_m = (int(now.year) * 12 + int(now.month) - mon) % 12
    last_y = int((int(now.year) * 12 + int(now.month) - mon) / 12)
    last_mon = '%s-%s' % (last_y, last_m)

    return last_mon


# 当前时间 1 个月前
last_13mon = get_date_month(1)

# 当前时间20年前
last_20year = int(now.year) - 20;

# 获取去年同期
last_year = int(now.year) - 1
last_1year_date = date_now.replace(year=last_year)

print(now)
print(date_now)
print(last_13mon)
print(last_20year)
print(last_1year_date)
