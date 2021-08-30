# -*- coding: utf-8 -*-

"""
Created on Created on 2021-08-02

@author: Wang Shuo
"""


class BaseException(Exception):

    def __init__(self, message):
        super().__init__(self)
        self.__message = message

    def __str__(self):
        return str(self.__message)


class ReportException(BaseException):
    pass


class DataException(BaseException):
    pass
