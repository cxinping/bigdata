# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch

def create_index():
    es = Elasticsearch(['192.168.11.10:9200'] )
    result = es.indices.create(index='news', ignore=400)
    print(result)

def delete_index():
    es = Elasticsearch(['192.168.11.10:9200'])
    result = es.indices.delete(index='news', ignore=[400, 404])
    print(result)

def create_data():
    es = Elasticsearch(['192.168.11.10:9200'])
    es.indices.create(index='news', ignore=400)

    data = {'title': '测试数据', 'url': 'http://www.163.com'}
    result = es.create(index='news', doc_type='politics', id=3, body=data)
    print(result)

def update_data():
    es = Elasticsearch(['192.168.11.10:9200'])
    data = {'title': '测试数据aaa',
            'url': 'http://www.163.com'
            }
    result = es.update(index='news', doc_type='politics', body=data, id=1)
    print(result)

def delete_data():
    es = Elasticsearch(['192.168.11.10:9200'])
    result = es.delete(index='news', doc_type='politics', id=1)
    print(result)

def create_index_cn():
    es = Elasticsearch(['192.168.11.10:9200'])
    mapping = {
        'properties': {
            'title': {
                'type': 'text',
                'analyzer': 'ik_max_word',
                'search_analyzer': 'ik_max_word'
            }
        }
    }
    es.indices.delete(index='news', ignore=[400, 404])
    es.indices.create(index='news', ignore=400)
    result = es.indices.put_mapping(index='news', doc_type='politics', body=mapping)
    print(result)

if __name__ == '__main__':
    #create_index()
    #create_data()
    delete_index()
    create_index_cn()







