# -*- coding: utf-8 -*-
import asyncio, aiomysql
from report.commons.mysql_pool import AsyncMysql, exec_insert
from report.commons.logging import get_logger

log = get_logger(__name__)

def select_demo1():
    from datetime import datetime
    log.info(' --- begin ---')
    sqllist = ['select id, area_name, city,province  from ( select * from areas ) t ']
    x = datetime.now()
    event_loop = asyncio.get_event_loop()
    task = event_loop.create_task(exec_insert(event_loop, sqltype='select', sqllist=sqllist))
    event_loop.run_until_complete(task)

    event_loop.close()
    log.info('共耗时' + str(datetime.now() - x))

    results = task.result()
    count_num = -1
    if results:
        for rs in results:
            print(rs)



select_demo1()

