# -*- coding: utf-8 -*-
'''
Created on

@author:
'''

DEBUG = True

# configure database information
SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://xinping:123@Welcome@192.168.11.10:3306/RRAD1?charset-utf8'
SQLALCHEMY_TRACK_MODIFICATIONS = False
SQLALCHEMY_ECHO = True

SQLALCHEMY_BINDS = {
    'rra':   'mysql+pymysql://xinping:123@Welcome@192.168.11.10:3306/RRAD2?charset-utf8'
}






