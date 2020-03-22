#!/usr/bin/python
# encoding=utf8
# bdi流程解析
# by hyn
# 20200317

from xml.dom.minidom import parse
import xml.dom.minidom

def test():
    # 使用minidom解析器打开 XML 文档
    DOMTree = xml.dom.minidom.parse("./flow_Output20200317103752/flow_Output20200317103752.xml")
    collection = DOMTree.documentElement

    # # 在集合中获取所有电影
    # movies = collection.getElementsByTagName("TENANT_ID")
    # print(movies)
    # print(movies[0])
    #
    # # 获取标签值
    # print(movies[0].firstChild.data)

    movies = collection.getElementsByTagName("TENANT_ID")
    print(movies)
    print(movies[0])


    




test()