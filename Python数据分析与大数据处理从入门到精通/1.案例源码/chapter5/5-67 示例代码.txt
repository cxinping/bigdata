# -*- coding: UTF-8 -*-


class Poultry:
    def eat(self):
        print("这是父类：poultry的方法")

class Chicken:
    def eat(self):
        print("这是父类：Chicken的方法")

class Cock(Poultry, Chicken):
    def invoke_eat(self):
        self.eat()

class Cock1(Chicken, Poultry):
    def invoke_eat(self):
        self.eat()

cock = Cock()
cock.eat()
cock1 = Cock1()
cock1.eat()


