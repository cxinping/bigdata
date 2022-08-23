# -*- coding: utf-8 -*-
from django.http import HttpResponse
from app01.models import Person

# 数据库操作
def testdb(request):
    person = Person(name='codebaoku', address='北京')
    person.save()
    return HttpResponse("<p>数据添加成功！</p>")