# -*- coding: utf-8 -*-

from celery_task import send_email

result = send_email.delay("wang")
print('result.id=',result.id)

result2 = send_email.delay("li")
print('result2.id=', result2.id)


