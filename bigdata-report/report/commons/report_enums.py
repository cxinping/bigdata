# -*- coding: utf-8 -*-

from enum import Enum


class ArimaType(Enum):
    """ 预测类型 类型 """
    BETWEEN_DATE = 'between_date'
    MEMBER_CONF = 'member_cont'


if __name__ == '__main__':
    print(ArimaType.BETWEEN_DATE.value)
    print(ArimaType.BETWEEN_DATE == ArimaType.BETWEEN_DATE)
