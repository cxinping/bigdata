# -*- coding: UTF-8 -*-


class Chicken:
    def __init__(self):
        self.weight = "3.0kg"

    def get_weight(self):
        return self.weight

    def set_weight(self, val):
        self.weight = val

    def del_weight(self):
        del self.weight

    p_weight = property(get_weight, set_weight, del_weight)


chicken = Chicken()
print("获取weight值：", chicken.p_weight)
chicken.p_weight = "5.0kg"
print("设置weight值：", chicken.p_weight)
del chicken.weight
try:
    print("获取删除weight属性后的值：", chicken.weight)
except Exception as e:
    print("访问删除后的weight属性，触发异常：", e)

	
	