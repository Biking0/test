#!/usr/bin/env python
# -*-coding:utf-8 -*-
# 20210224

import requests
from lxml import etree
import re

url = "http://wquan.moojing.com/quiz/index.html"


# 解析item list
def item_list():
    res = requests.get(url)
    html = etree.HTML(res.text)

    item_list = html.xpath('//div/li/text()')

    print(item_list)


def ajax_result():
    ajax_url = "http://wquan.moojing.com/get_data?itemid=595843737123&time=1614185282440&sign=:%3E:=98%3C8%3C678jfx~6;696=:7=7995"
    # ajax_url = "http://wquan.moojing.com/get_data?itemid=12345678&time=1614185282440&sign=:>:=98<8<678jfx~6;696=;==>=7>"

    headers = {'Accept': '*/*',
               'Accept-Encoding': 'gzip, deflate',
               'Accept-Language': 'zh-CN,zh;q=0.9',
               'Connection': 'keep-alive',
               'Host': 'wquan.moojing.com',
               'Referer': 'http://wquan.moojing.com/quiz/index.html',
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
               'X-Requested-With': 'XMLHttpRequest'}

    res = requests.get(ajax_url, headers=headers)

    print(res.text)

    pattern = re.compile(r'([skuId]+) ([=]+)', re.I)
    m = pattern.match(res.text)
    print(m)
    # print(re.match('skuId', 'www.runoob.com'))


# item_list()
ajax_result()
