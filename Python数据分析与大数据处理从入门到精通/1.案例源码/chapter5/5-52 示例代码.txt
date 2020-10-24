# -*- coding: UTF-8 -*-

class Poultry:
    def fly(self):
        print("这是父类：poultry的方法")

class Chicken:
    def eat(self):
        print("这是父类：Chicken的方法")

class Cock(Poultry, Chicken):
    pass

class Duck:
    def fly(self):
        print("这是类：duck的fly方法")

def run(poultry):
    poultry.fly()

cock = Cock()
duck = Duck()
run(cock)
run(duck)

