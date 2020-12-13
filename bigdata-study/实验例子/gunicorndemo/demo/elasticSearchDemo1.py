# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch

def create_index():
    es = Elasticsearch(['192.168.11.10:9200'] )
    result = es.indices.create(index='news', ignore=400)
    print(result)

if __name__ == '__main__':
    create_index()









