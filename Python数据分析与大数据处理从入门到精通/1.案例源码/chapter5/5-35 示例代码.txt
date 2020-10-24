# -*- coding: UTF-8 -*-


class Chicken:
    weight = 0.5

    def __init__(self, num):
        self._num = num

    @staticmethod
    def get_weight():
        print("小鸡重量 {0} kg:".format(Chicken.weight))

    @staticmethod
    def run(num):
        print("小鸡 {0} 跑起来:".format(num))

Chicken.get_weight()
Chicken.run("Num1")
print()
chicken1 = Chicken("Num2")
chicken1.get_weight()
chicken1.run("Num2")
print()
chicken2 = Chicken("Num3")
chicken2.get_weight()
chicken2.run("Num3")
print()
print("类的run方法：",id(Chicken.run))
print("实例 chicken1 run方法：",id(chicken1.run))
print("实例 chicken2 run方法：",id(chicken2.run))

