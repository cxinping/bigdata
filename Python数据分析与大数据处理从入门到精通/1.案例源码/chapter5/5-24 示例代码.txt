# -*- coding: UTF-8 -*-
class Chicken:
    def __init__(self):
        self.weight = None

    def get_weight(self):
        return self.weight

    def set_weight(self, val):
        if not isinstance(val, float):
            raise ValueError("请传递一个整数或浮点数！")
        if 0.1 > val or val > 12:
            raise ValueError("请确保参数范围在0.1到12之间！")

        self.weight = val

chicken = Chicken()
try:
    chicken.set_weight("abc")
except Exception as e:
    print("传递字符串，触发异常：", e)

try:
    chicken.set_weight(20)
except Exception as e:
    print("传递数据超出范围，触发异常：", e)

chicken.set_weight(8.5)
print("获取正常的weight：", chicken.get_weight())

