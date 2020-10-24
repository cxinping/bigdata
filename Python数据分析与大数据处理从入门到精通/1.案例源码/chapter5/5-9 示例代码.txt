# -*- coding: UTF-8 -*-
from abc import ABCMeta

class Poultry(metaclass=ABCMeta):
    colour = "白色"
    num = "num1"
    
poultry = Poultry()
print("对象地址：", id(poultry))
print("对象类型：", type(poultry))


