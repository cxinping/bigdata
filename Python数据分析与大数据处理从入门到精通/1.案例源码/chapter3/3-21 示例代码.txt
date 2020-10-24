# -*- coding: UTF-8 -*-
from string import Template

c = Template("${a},${b}")
d = c.substitute(a="Hello", b="World")
print("使用模板产生的字符：", d)


