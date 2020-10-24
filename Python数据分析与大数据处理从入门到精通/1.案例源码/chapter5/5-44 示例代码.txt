# -*- coding: UTF-8 -*-


class Chicken:
    weight = 0.5

    def __init__(self, num):
        self._num = num

    @staticmethod
    def get_weight():
        print("小鸡重量 {0} kg:".format(Chicken.weight))


def run(num):
    print("小鸡 {0} 跑起来:".format(num))


Chicken.run = run
Chicken.run("Num1")


