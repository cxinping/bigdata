# -*- coding: utf-8 -*-

from report.services.travel_expense_service import *


def demo1():
    names = get_travel_keyword()
    for name in names:
        print(name)


if __name__ == '__main__':
    demo1()
