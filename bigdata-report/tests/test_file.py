# -*- coding: utf-8 -*-
f = "/my_filed_algos/lucky.txt"

a = 21
with open(f,"w") as file:   #”w"代表着每次运行都覆盖内容
    for i in range(a):
        file.write(str(i) + "d" + " "+"\n")
    a +=1