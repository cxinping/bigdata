# -*- coding: utf-8 -*-
from report.commons.commons import *
from report.commons.tools import *

# import datetime
#
# def getTheMonth(date, n):
#     month = date.month
#     year = date.year
#     for i in range(n):
#         if month == 1:
#             year -= 1
#             month = 12
#         else:
#             month -= 1
#     return datetime.date(year, month, 1).strftime('%Y%m')
#
#
# date = datetime.datetime.today()
# single_time_new = getTheMonth(date, 1)
# single_time_old = getTheMonth(date, 2)
#
# print('single_time_new=', single_time_new)
# print('single_time_old=', single_time_old)

def demo1():
    time1 = get_date_month(1)
    print(time1)

def demo2():
    t = get_current_time()
    print(t)

if __name__ == '__main__':
    demo2()
