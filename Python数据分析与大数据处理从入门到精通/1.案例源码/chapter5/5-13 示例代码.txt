# -*- coding: UTF-8 -*-

class Chicken:
    colour = "白色"
    num = "num1"

chicken = Chicken()
print("通过类名访问属性 colour：", Chicken.colour)
print("通过类名访问属性 num：", Chicken.num)
print()
print("通过实例访问属性 colour：", chicken.colour)
print("通过实例访问属性 num：", chicken.num)


