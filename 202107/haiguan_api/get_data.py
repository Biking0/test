#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：get_data.py
# 功能描述：海关接口交互
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20210714
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python get_data.py
# ***************************************************************************

# 1.获取token
# 2.新增一键排查
# 3.任务列表
# 4.销售链路图 树状图

import requests
import json

token_url = "http://223.4.72.49/zsasite-test/zsaoauth/oauth/token"
# token_url="https://10.73.1.66:9091/test/bigdatacoldchain/zsaoauth/oauth/token"

paicha_url = "http://223.4.72.49/zsasite-test/zsathird/api/queueMessageLL/insertQueueMessage"
# paicha_url="https://10.73.1.66:9091/test/bigdatacoldchain/zsathird/api/queueMessageLL/insertQueueMessage"

task_url = "http://223.4.72.49/zsasite-test/zsathird/api/queueMessageLL/selectMessageList"
# task_url="https://10.73.1.66:9091/test/bigdatacoldchain/zsathird/api/queueMessageLL/selectMessageList"

xiaoshou_url = "http://223.4.72.49/zsasite-test/zsathird/api/queueMessageLL/selecTreeList"


# xiaoshou_url="https://10.73.1.66:9091/test/bigdatacoldchain/zsathird/api/queueMessageLL/selecTreeList"


# 获取token
def get_token():
    payload = 'client_id=testclient&client_secret=31ebefaee5594efa87695513c7ebad3b&grant_type=password&username=test&password=test1234'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", token_url, headers=headers, data=payload)

    print(response.text)

    result_json = json.loads(response.text)

    print(result_json['access_token'])

    access_token = result_json['access_token']

    return get_paicha(access_token)




# 请求一键排查
def get_paicha(access_token):
    payload = 'regulatoryCode=2900&createUser=2992750&createName=%E5%AD%99%E5%B8%86&titile=%E6%B5%8B%E8%AF%950708&beginTime=2021-01-01&endTime=2021-07-01&inspectionCertificateNo=119000007585181'
    headers = {
        'Authorization': 'bearer ' + access_token,
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", paicha_url, headers=headers, data=payload)

    print(response.text)

    result_json = json.loads(response.text)
    code = str(result_json['data'])

    return get_task(access_token, code)


# 请求任务列表
def get_task(access_token, code):
    test_code = '2900'

    # payload = 'regulatoryCode='+code+'&page=1&pageSize=100'
    payload = 'regulatoryCode=' + test_code + '&page=1&pageSize=100'
    headers = {
        'Authorization': 'bearer ' + access_token,
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", task_url, headers=headers, data=payload)

    print(response.text)

    result_json = json.loads(response.text)

    data = result_json['data']

    print(len(data['rows']))

    rows_data = data['rows']

    for i in rows_data:

        if i['id'] == int(code):
            schedule = i['schedule']
            print(i['schedule'])
            print(type(i['schedule']))
            print(i)

            if schedule == 2:
                get_xiaoshou(access_token, code)

            return get_xiaoshou(access_token, code)


# 销售链路图 树状图
def get_xiaoshou(access_token, code):
    # payload = 'id='+code
    payload = 'id='+'3042'
    headers = {
        'Authorization': 'bearer ' + access_token,
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': 'JSESSIONID=D20D99A63995B6E4D9D873B321D37BFD; SERVERID=5d0a716c441ba7fdfc199a9f5c095cb5|1626278121|1626277094'
    }

    response = requests.request("POST", xiaoshou_url, headers=headers, data=payload)

    print(response.text)

    return json.loads(response.text)

# get_token()
