# -*- coding: utf-8 -*-
def generate():
    i = 0
    while i < 5:
        print("我在这。。")
        xx = yield i  # 注意，python程序，碰到=，都是先从右往左执行
        print(xx)
        i += 1

g = generate()

g.send(None)

g.send("lalala")




