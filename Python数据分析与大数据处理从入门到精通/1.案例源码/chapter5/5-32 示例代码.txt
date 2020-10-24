# -*- coding: UTF-8 -*-

class Chicken:

    def __init__(self, num):
        self._num = num

    def fly(self):
        print("小鸡 {0} 飞起来".format(self._num))

    def run(self):
        print("小鸡 {0} 跑起来".format(self._num))

chicken1 = Chicken("Num1")
chicken1.fly()
chicken1.run()
print()
chicken2 = Chicken("Num2")
chicken2.fly()
chicken2.run()


