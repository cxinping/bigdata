# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from django.db import models

# 新增元组用于设置消息类型枚举项
KIND_CHOICES = (
    ('Python技术', 'Python技术'),
    ('数据库技术', '数据库技术'),
    ('经济学', '经济学'),
    ('文体资讯', '文体资讯'),
    ('个人心情', '个人心情'),
    ('其他', '其他'),
)

# Create your models here.


class Moment(models.Model):
    content = models.CharField(max_length=300)
    user_name = models.CharField(max_length=20, default='匿名')
    # 修改kind定义，加入choices参数
    kind = models.CharField(
        max_length=20, choices=KIND_CHOICES, default=KIND_CHOICES[0])


LEVELS = (
    ('1', 'Very good'),
    ('2', 'Good'),
    ('3', 'Normal'),
    ('4', 'Bad'),
)


class Comment(models.Model):
    id = models.AutoField(primary_key=True)
    level = models.CharField("请为本条信息评级", max_length=1, choices=LEVELS)
