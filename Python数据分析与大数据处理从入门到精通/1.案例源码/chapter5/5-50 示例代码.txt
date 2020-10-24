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
    def fly(self):
        print("这是子类：Cock的fly方法")

    def eat(self):
        print("这是子类：Cock的eat方法")


cock = Cock("黄色")
print("访问_colour属性：", cock._colour)
cock.fly()
cock.eat()

