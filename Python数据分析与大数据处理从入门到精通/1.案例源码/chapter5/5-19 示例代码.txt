# -*- coding: UTF-8 -*-


class Chicken:
    colour = "白色"
    num = "num1"

    def __init__(self, weight, price):
        self.weight = weight
        self.price = price


chicken1 = Chicken("2.5kg", "￥80")
chicken2 = Chicken("3kg", "￥100")

print("chicken1 weight 地址：", id(chicken1.weight))
print("chicken1 price 地址：", id(chicken1.price))
print()
print("chicken2 colour 地址：", id(chicken2.weight))
print("chicken2 price 地址：", id(chicken2.price))
print()

chicken1.weight = "3.5kg"
chicken1.price = "￥120"
chicken1.weight = "2.0kg"
chicken1.price = "￥60"

print("实例 chicken1 访问修改后的属性 weight：", chicken1.weight)
print("实例 chicken1 访问修改后的属性 price：", chicken1.price)
print()
print("实例 chicken2 访问修改后的属性 weight：", chicken2.weight)
print("实例 chicken2 访问修改后的属性 price：", chicken2.price)

