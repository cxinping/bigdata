# -*- coding: UTF-8 -*-

class Poultry:
    def __init__(self, colour):
        self._colour = colour

    def fly(self):
        print("这是父类：poultry的方法")

class Chicken(Poultry):
    pass

chicken = Chicken("黄色")
print("访问_colour属性：", chicken._colour)
chicken.fly()


