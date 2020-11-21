### Python 关于季度时间计算

* 根据月份获取季度
* 获取两个时间之间的季度时间
* 获取临近几个季度的时间(上一个季度，下一个季度)
* 获取本季度第一天
* 获取本季度最后一天

```
import calendar
import datetime
import time
 
def get_week():
 from datetime import datetime
 week = datetime.strptime('20201121', '%Y%m%d').weekday()
 print(week)

 
def getBetweenMonth(begin_date, end_date=None):
    ''' 如何判断给定的日期是周几 '''
    date_list = []
    begin_date = datetime.datetime.strptime(begin_date, "%Y%m%d")
    print(begin_date)
    end_date = end_date if end_date else datetime.datetime.strptime(
        time.strftime('%Y%m%d', time.localtime(time.time())), "%Y%m%d")
    while begin_date <= end_date:
        date_str = begin_date.strftime("%Y-%m")
        date_list.append(date_str)
        begin_date = add_months(begin_date, 1)
    return date_list
 
 
def add_months(dt, months):
    month = dt.month - 1 + months
    year = int(dt.year + month / 12)
    month = int(month % 12 + 1)
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return dt.replace(year=year, month=month, day=day)
 
 
def get_quarter(month_list):
    '''
    根据月份获取季度
    :param month_list: ['2018-11', '2018-12']
    :return:日期所在季度月末值
    '''
    quarter_list = []
    for value in month_list:
        temp_value = value.split("-")
        if temp_value[1] in ['01', '02', '03']:
            quarter_list.append(temp_value[0] + "0331")
        elif temp_value[1] in ['04', '05', '06']:
            quarter_list.append(temp_value[0] + "0630")
        elif temp_value[1] in ['07', '08', '09']:
            quarter_list.append(temp_value[0] + "0930")
        elif temp_value[1] in ['10', '11', '12']:
            quarter_list.append(temp_value[0] + "1231")
        quarter_set = set(quarter_list)
        quarter_list = list(quarter_set)
        quarter_list.sort()
    return quarter_list
 
 
def getBetweenQuarter(begin_date, end_date=None):
    '''
    获取两个时间之间的季度时间
    :param begin_date: 起始时间
    :param end_date: 默认当前时间
    :return:  日期所在季度月末值
    '''
    month_list = getBetweenMonth(begin_date, end_date)
    quarter_list = get_quarter(month_list)
    return quarter_list
 
 
def getNearQuarter(begin_date, num=1):
    '''
    获取临近几个季度的截止时间
    :param begin_date: 起始时间
    :param num: 正整数向将来推   默认:下一个季度
    :return: 日期所在季度月末值
    '''
    month_list = []
    begin_date = datetime.datetime.strptime(begin_date, "%Y%m%d")
    begin_date_ = add_months(begin_date, 3 * num)
    date_str = begin_date_.strftime("%Y-%m")
    month_list.append(date_str)
    quarter_list = get_quarter(month_list)
 
    return quarter_list
 
 
def getQuarterFirstDay(date_str):
    '''
    获取本季度第一天
    :param date_str:
    :return:日期所在季度第一天
    '''
    date = datetime.datetime.strptime(date_str, "%Y%m%d")
    quarter_start_day = datetime.date(date.year, date.month - (date.month - 1) % 3, 1)
    return quarter_start_day.isoformat()
 
 
def getQuarterLastDay(date_str):
    '''
    本季度最后一天
    :param date_str:
    :return: 日期所在季度最后一天
    '''
    from dateutil.relativedelta import relativedelta  # 引入新的包
    date = datetime.datetime.strptime(date_str, "%Y%m%d")
    quarter_end_day = datetime.date(date.year, date.month - (date.month - 1) % 3 + 2, 1) + relativedelta(months=1,
                                                                                                         days=-1)
    return quarter_end_day.isoformat()
 
 
if __name__ == '__main__':
    begin_date = '20181121'
    quarter_list = getBetweenQuarter(begin_date, end_date=None)
    # quarter_list = getNearQuarter(begin_date)
    print(quarter_list)
    date_str = '20180630'
    # print(getQuarterFirstDay(date_str))
    getQuarterLastDay(data_str)

```


