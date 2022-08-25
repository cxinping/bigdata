# -*- coding: utf-8 -*-
from django.http import HttpResponse
from app01.models import Person
from mycelery.sms.tasks import send_sms,send_sms2

# 数据库操作
def testdb(request):
    # person = Person(name='codebaoku', address='北京')
    # person.save()

    send_sms.delay("110")
    return HttpResponse("<p>数据添加成功！</p>")