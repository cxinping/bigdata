# -*- coding: utf-8 -*-
from report.services.data_process_services import *


def demo1():
    process = BaseProcess()
    process.exec_step08()


def demo2():
    records = query_temp_performance_bill(None)
    print(len(records))
    for record in records:
        print(record)
        print()


def demo3():
    full_process = FullAddProcess()
    # full_process.exec_step05()
    full_process.exec_step06()


def demo4():
    records = query_temp_performance_bill_by_target_classify("差旅费")
    #print(len(records))

    for record in records:
        sql = str(record[0])
        order_number = str(record[1])
        #print(order_number)
        if order_number == '09':
            print(sql)
            print()
            prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)


if __name__ == '__main__':
    # demo1()
    # demo2()
    # demo3()
    demo4()
