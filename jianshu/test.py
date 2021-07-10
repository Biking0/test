# encoding=utf8
# demo test for tender spider
# by hyn
# 20200106


import urllib.request
import simplejson

import requests
# import tender_spider.test.parser_data as parser_data

# url = 'http://www.hngp.gov.cn/henan/list2?channelCode=0101&pageSize=16&bz=1&gglx=0&pageNo=1'
url = 'https://www.jianshu.com/p/35fa627ccc34'
# response = requests.request("GET", url, headers=headers)

headers = {
    # 'host': "makeabooking.flyscoot.com",
    # 'content-length': "831",
    # 'cache-control': "no-cache",
    # 'Origin': "http://qyn62.com",
    # 'upgrade-insecure-requests': "1",
    # 'content-type': "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW",
    'User-Agent': "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
    # 'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    # 'Referer': "http://qyn62.com/details.html?label=MTQzMjU4MTk5OC5yc2MuY2RuNzcub3JnLzE1NDUyMjE0MDEvNDgwUF82MDBLXzE0NTUwNzQyMi5tM3U4&search=MjA1NQ==&name=JUU3JUJFJThFJUU1JUFFJUI5JUU5JTk5JUEyJUU4JTgwJTgxJUU2JTlEJUJGJUU1JUE4JTk4JUU3JTk2JUFGJUU3JThCJTgyJUU3JTk0JUJCJUU5JTlEJUEyJUU2JTgwJUE3JUU2JTg0JTlGJUU3JTk5JUJEJUU4JUExJUEzJUU4JUEzJTk5&label=JUU1JTlCJUJEJUU0JUJBJUE3JUUzJTgwJTgxJUU2JTgwJUE3JUU2JTg0JTlGJUU5JUJCJTkxJUU0JUI4JTlEJUUzJTgwJTgxJUU2JUI3JUFCJUU4JThEJUExJUU3JTg2JTlGJUU1JUE1JUIzJUUzJTgwJTgxJUU1JTgxJUI3JUU2JTgzJTg1",
    # 'accept-encoding': "gzip, deflate, br",
    # 'accept-language': "zh-CN,zh;q=0.9",
    'Cookie': "__yadk_uid=aSwDTtFSZR3QHeL94zVL8qezjATkcdlu; __gads=ID=6f8c5c856dc69f19:T=1579885342:S=ALNI_MYGXq8bCHMUQdv-2czd-Cuo6VfIvw; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2216e22272b4127a-0ce97b52718f9a-7711439-1049088-16e22272b42590%22%2C%22%24device_id%22%3A%2216e22272b4127a-0ce97b52718f9a-7711439-1049088-16e22272b42590%22%2C%22props%22%3A%7B%22%24latest_referrer%22%3A%22https%3A%2F%2Fwww.baidu.com%2Flink%22%2C%22%24latest_traffic_source_type%22%3A%22%E8%87%AA%E7%84%B6%E6%90%9C%E7%B4%A2%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC%22%7D%2C%22first_id%22%3A%22%22%7D; _ga=GA1.2.956267339.1581668521; read_mode=day; default_font=font2; locale=zh-CN; _gid=GA1.2.439312722.1583658141; Hm_lvt_0c0e9d9b1e7d617b3e6842e85b9fb068=1583299354,1583336503,1583394345,1583658141; _m7e_session_core=f1d888a08bd17953b0f3b87a47e99bd0; _gat=1; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2216e22272b4127a-0ce97b52718f9a-7711439-1049088-16e22272b42590%22%2C%22%24device_id%22%3A%2216e22272b4127a-0ce97b52718f9a-7711439-1049088-16e22272b42590%22%2C%22props%22%3A%7B%22%24latest_referrer%22%3A%22https%3A%2F%2Fwww.baidu.com%2Flink%22%2C%22%24latest_traffic_source_type%22%3A%22%E8%87%AA%E7%84%B6%E6%90%9C%E7%B4%A2%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC%22%7D%2C%22first_id%22%3A%22%22%7D; signin_redirect=https://www.jianshu.com/p/35fa627ccc34; Hm_lpvt_0c0e9d9b1e7d617b3e6842e85b9fb068=1583667897",
    # 'connection': "keep-alive",

}

for i in range(5):
    # response = requests.request("GET", url)

    response = requests.request("GET", url, headers=headers)


    # response=requests.get(url)
    print(response.text)
    print('##################', i)
    # parser_data.parser(response.text)
