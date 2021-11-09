# -*- coding: utf-8 -*-

import asyncio, aiomysql

from report.commons.logging import get_logger

log = get_logger(__name__)

DB_HOST = '10.5.138.11'
DB_USER = 'root'
DB_PASSWD = '123456'
DB_NAME = 'report'
DB_PORT = 13306
DB_MINSIZE = 1
DB_MAXSIZE = 100
DB_COMMIT = True
DB_CHARSET = 'utf8mb4'


class AsyncMysql(object):
    def __init__(self, loop):
        self._host = DB_HOST
        self._port = DB_PORT
        self._db = DB_NAME
        self._user = DB_USER
        self._password = DB_PASSWD
        self._minsize = DB_MINSIZE
        self._maxsize = DB_MAXSIZE
        self._autocommit = DB_COMMIT
        self._charset = DB_CHARSET
        self._pool = None
        self._loop = loop

    async def get_db_pool(self):
        log.info('begin create pool')
        self._pool = await aiomysql.create_pool(host=self._host, port=self._port,
                                                user=self._user, password=self._password,
                                                cursorclass=aiomysql.DictCursor,
                                                db=self._db, loop=self._loop, autocommit=self._autocommit,
                                                charset='utf8')
        log.info('create_aiomysql_pool success')

    '''外面调用这个方法，传sql列表一次500-1000个sql即可'''

    async def exe_sql_task(self, sqltype='insert', sql_list=[]):
        if isinstance(sql_list, list):
            if sqltype == 'insert':
                task_list = [self.exe_sql(sql=sql) for sql in sql_list]
            elif sqltype == 'select':
                task_list = [self.exe_sql(sqltype=sqltype, sql=sql) for sql in sql_list]
            sql_list = None
            if len(task_list) > 0:
                log.info('start execute sql')
                result, pending = await asyncio.wait(task_list)
                log.info('execute sql down')
                if pending:
                    log.info('canceling tasks')
                    log.error(pending)
                    for t in pending:
                        t.cancel()
                if result:
                    return result
                else:
                    return None
            else:
                log.info('Error constructing SQL list')
        else:
            log.error('exe_sql_task方法传入的sql与param应为list')

    async def exe_sql(self, sqltype='insert', sql=''):
        if self._pool is None:
            await self.get_db_pool()
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                result = None
                try:
                    if sqltype == 'insert':
                        await cur.execute(sql)
                        await conn.commit()  # ws
                    elif sqltype == 'select':
                        await cur.execute(sql)
                        result = await cur.fetchall()
                except Exception as ex:
                    if self._autocommit == False:
                        if conn:
                            await conn.rollback()
                    ''' 新增代码将错误记录写入数据库，唯一ID、sql、异常信息三列即可 '''
                    log.info('SQL：{}\n 执行异常，错误原因为：'.format(sql))
                    log.error(ex)
                    return None
                return result

    async def __aenter__(self):
        if self._pool is None:
            await self.get_db_pool()
        return self

    async def _close(self):
        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()
            log.info('close_aiomysql_pool success')

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._close()


async def exec_insert(event_loop, sqltype='insert', sqllist=[]):
    async with AsyncMysql(event_loop) as ae:
        results = await ae.exe_sql_task(sqltype, sqllist)
    return results


def insert_demo1():
    sqllist = []
    sql = 'insert into areas(area_name,city, province) values("朝阳" , "北京市", "北京市")'
    sqllist.append(sql)

    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(exec_insert(event_loop, sqltype='insert', sqllist=sqllist))
    event_loop.close()


def select_demo1():
    from datetime import datetime
    log.info(' --- begin ---')
    sqllist = ['select id, area_name, city,province from areas ']
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


if __name__ == "__main__":
    pass

    #insert_demo1()
    select_demo1()


