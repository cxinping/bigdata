# -*- coding: UTF-8 -*-

class Poultry:
    def __init__(self, colour):
        self._colour = colour

    def fly(self):
        print("这是父类：poultry的方法")

class Chicken:
    def eat(self):
        print("这是父类：Chicken的方法")

class Cock(Poultry, Chicken):
    pass

cock = Cock("黄色")
print("访问_colour属性：", cock._colour)
cock.fly()
cock.eat()


