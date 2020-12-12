# -*- coding: utf-8 -*-
'''
Created on

@author:
'''

DEBUG = False

# Configure database information
SQLALCHEMY_DATABASE_URI = 'postgresql://postgres:123456@127.0.0.1/testdb2'
SQLALCHEMY_TRACK_MODIFICATIONS = False
SQLALCHEMY_ECHO = False

SQLALCHEMY_BINDS = {
    'rra':   'oracle://LREQUERY:Dti_202011@10.119.61.174:1521/RRAD1'
}



