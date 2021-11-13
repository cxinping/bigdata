# -*- coding: utf-8 -*-
g = {
  'x': 1,
  'y': 2
}
l = {}

exec('''
global x,z
x=100
z=200

m=300
''', g, l)

print('* g=',g)  # {'x': 100, 'y': 2,'z':200,......}
print('* l=',l)  # {'m': 300}

print('=' * 50)

r = 'print(x+y)'
r1 = """
result = x + y
print(result)
"""
val = {'x' : 1, 'y':2}
exec(r1,globals(), val)
print(val)