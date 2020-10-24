# -*- coding: UTF-8 -*-

class WildGoose:
    def __init__(self):
        self.direction = []

    def __call__(self, direction):
        self.direction.append(direction)
        print("大雁正往 {0} 方向飞".format(direction))

    def __str__(self):
        return str.join("--->", self.direction)


wild_goose = WildGoose()
wild_goose("上海")
wild_goose("杭州")
wild_goose("宜春")
wild_goose("南京")
wild_goose("广州")
print("大雁飞行轨迹：", str(wild_goose))


