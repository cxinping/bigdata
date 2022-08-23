from django.db import models

from django.db import models


class Person(models.Model):
    name = models.CharField(max_length=20)
    address = models.CharField(max_length=50)