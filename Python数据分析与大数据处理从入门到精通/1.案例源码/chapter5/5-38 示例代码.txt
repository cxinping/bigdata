# -*- coding: UTF-8 -*-


class Chicken:
    colour = "黄色"
    count = 0

    def __init__(self, colour):
        Chicken.count = Chicken.count + 1

    @classmethod
    def get_colour(cls):
        print("cls：", cls)
        print("访问类属性，count值为：{0}".format(cls.count))



Chicken.get_colour()
print()
chicken1 = Chicken("Num2")
chicken1.get_colour()
print()
chicken2 = Chicken("Num3")
chicken2.get_colour()
print()
print("类的get_colour方法：",id(Chicken.get_colour))
print("实例 chicken1 get_colour方法：",id(chicken1.get_colour))
print("实例 chicken2 get_colour方法：",id(chicken2.get_colour))



