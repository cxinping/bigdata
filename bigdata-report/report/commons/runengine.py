# -*- coding: utf-8 -*-
import threading
from report.commons.logging import get_logger
from report.commons.tools import get_current_time
from report.services.common_services import (insert_finance_shell_daily, update_finance_shell_daily)
import traceback
from report.commons.connect_kudu import prod_execute_sql
from multiprocessing import Process

log = get_logger(__name__)

RUNENGINE_MAX_THREADS = 10
threadLock = threading.Lock()
threads = []
max_threads = threading.Semaphore(RUNENGINE_MAX_THREADS)


def create_new_thread(target):
    def wrapper(*args, **kwargs):
        max_threads.acquire()
        t = threading.Thread(target=target, args=args, kwargs=kwargs)
        threads.append(t)
        t.start()

    return wrapper


def create_new_process(target):
    def wrapper(*args, **kwargs):
        # max_threads.acquire()
        t = threading.Thread(target=target, args=args, kwargs=kwargs)
        # threads.append(t)
        # t.start()
        p = Process(target=target, args=args, kwargs=kwargs)
        p.start()

    return wrapper


# 另开线程，防止主线程阻塞
@create_new_thread
def execute_task(isalgorithm, unusual_shell, unusual_id):
    thr = threading.current_thread()
    log.info('Start executing task with new thread ' + thr.getName())

    if isalgorithm == '1':
        execute_kudu_sql(unusual_shell, unusual_id)
    elif isalgorithm == '2':
        execute_py_shell(unusual_shell, unusual_id)


def execute_py_shell(unusual_shell, unusual_id, mode='activate'):
    """
    执行检查点的 python shell 算法
    :param unusual_shell:
    :return:
    """

    try:
        # eval("print(1+2)")
        # exec('1/0')
        # print(unusual_shell)
        # exec("print('执行算法 shell 开始')")

        daily_start_date = get_current_time()

        daily_id = insert_finance_shell_daily(daily_status='ok', daily_start_date=daily_start_date,
                                              daily_end_date='', unusual_point=unusual_id,
                                              daily_source='python shell',
                                              operate_desc=f'成功执行检查点{unusual_id}的Python Shell', unusual_infor='',
                                              task_status='doing')

        if unusual_id in ['13', '14']:
            # 检查点13,14 测试
            rst_val = {'x': 1, 'y': 2}
            # exec(unusual_shell, globals(), rst_val)
            exec(unusual_shell, globals())
            #print(f'* rst_val={rst_val}')
        else:
            exec(unusual_shell, globals())
            pass

        # exec("print('执行算法 shell 结束')")
        daily_end_date = get_current_time()
        update_finance_shell_daily(daily_id, daily_end_date, task_status='done')

    except BaseException as e:
        print('--- execute_py_shell throw exception ---')
        # print(e)
        error_info = str(e)
        traceback.print_exc()
        daily_end_date = get_current_time()

        insert_finance_shell_daily(daily_status='error', daily_start_date=daily_start_date,
                                   daily_end_date=daily_end_date,
                                   unusual_point=unusual_id, daily_source='python shell', operate_desc='',
                                   unusual_infor=error_info, task_status='done')


def execute_kudu_sql(unusual_shell, unusual_id):
    # print(unusual_shell)

    try:
        daily_start_date = get_current_time()
        print('*** begin execute_kudu_sql ')
        prod_execute_sql(conn_type='test', sqltype='insert', sql=unusual_shell)
        daily_end_date = get_current_time()
        operate_desc = f'成功执行检查点{unusual_id}的SQL'
        print('*** end execute_kudu_sql ***')

        insert_finance_shell_daily(daily_status='ok', daily_start_date=daily_start_date, daily_end_date=daily_end_date,
                                   unusual_point=unusual_id, daily_source='sql', operate_desc=operate_desc,
                                   unusual_infor='', task_status='done')
    except Exception as e:
        print(e)
        insert_finance_shell_daily(daily_status='error', daily_start_date=daily_start_date,
                                   daily_end_date=daily_end_date,
                                   unusual_point=unusual_id, daily_source='sql', operate_desc='', unusual_infor=str(e),
                                   task_status='done')
