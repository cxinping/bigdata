from django.test import TestCase

from app01.models import Person
from django.test import Client

class ExampleTest(TestCase):

    def setUp(self):
        print('It is setUp ')

    def tearDown(self):
        print('It is tearDown ')

    def test_addition(self):
        def addition(x, y):
            return x + y

        self.assertEqual(addition(1, 1), 2, 'ass is failed')  # 断言函数加和运算

    def test_create_person(self):
        # person = Person(name='wangwu', address='beijing')
        # person.save()

        person = Person.objects.create(name='wangwu', address='beijing')
        print(person, type(person))

        print('--- ok ---')

    def test_call_request(self):
        c = Client()
        #response = c.post('/login/', {'username': 'john', 'password': 'smith'})
        response = c.get('http://127.0.0.1:8000/testdb/')
        print(response.status_code, response.content)