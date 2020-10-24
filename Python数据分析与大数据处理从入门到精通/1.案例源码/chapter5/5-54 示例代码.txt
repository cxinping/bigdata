

class WildGoose:
    def __call__(self, direction):
        print("大雁正往 {0} 方向飞".format(direction))


wild_goose = WildGoose()
wild_goose("东南")


