
### 继承实现

```
import abc

class Animal(metaclass=abc.ABCMeta):  # 统一所有子类的方法
    @abc.abstractmethod     # 该装饰器限制子类必须定义有一个名为 say 的方法
    def say(self):
        print('动物基本的发声...', end='')

class People(Animal):   # 但凡继承Animal的子类都必须遵循Animal规定的标准
    def say(self):
        #super().say()
        print('汪汪汪')

p = People()
p.say()
```


### 异常

```

class LreException(Exception):

    def __init__(self , message):
        self.message = message

    def __str__(self):
        return self.message

if __name__ == '__main__':
    try:
        a = 3
        b = 2

        if a > b:
            raise LreException('自定义异常')

        print('--- ok ---')
    except LreException as err:
        print('打印LreException异常信息: ' , err)

    except Exception as err:
        print('打印Exception异常信息: ' , err)

```
