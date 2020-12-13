# -*- coding: utf-8 -*-

HOST = '192.168.11.10'
PORT = '3306'
USERNAME = 'xinping'
PASSWORD = '123@Welcome'
DATABASE = 'RAD1'
DB_URI = 'mysql+pymysql://{username}:{password}@{host}:{port}/{db}?charset-utf8'.format(username=USERNAME, password=PASSWORD, host=HOST,  port=PORT,                      db=DATABASE)

print(DB_URI)