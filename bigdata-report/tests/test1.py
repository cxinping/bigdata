# -*- coding: utf-8 -*-
# import logging  # 引入logging模块
# logging.basicConfig(level=logging.DEBUG,
#                     format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')  # logging.basicConfig函数对日志的输出格式及方式做相关配置
# # 由于日志基本配置中级别设置为DEBUG，所以一下打印信息将会全部显示在控制台上
# logging.info('this is a loggging info message')
# logging.debug('this is a loggging debug message')
# logging.warning('this is loggging a warning message')
# logging.error('this is an loggging error message')
# logging.critical('this is a loggging critical message')

# from report.commons.logging import get_logger
#
# log = get_logger(__name__)
#
# log.info('ssss')

def func():
    print('in func1')
    yield 22

f = func()
print(f)
print(f)
print(f)
# print(func())
# print(func())
