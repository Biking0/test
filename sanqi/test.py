import requests

url = 'http://10.93.171.80:8080/admin/schedule/mainmonitor_hn_data.action?op=inst&opt_type=hour&jobFlowId=c88463b8-6c5c-4fab-b41c-685cbfb7db01&beginDate=&tasknum=-&name=DPS_DWD_ICA2_3&data_time_=20191020'

headers = {
    # 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    # 'User-Agent': 'Dalvik/1.6.0 (Linux; U; Android 4.4.2; SM-G955N Build/NRD90M)',
    # 'Host': 'book.hop.com',
    # 'Connection': 'Keep-Alive',
    # 'Accept-Encoding': 'gzip',
    # 'Content-Length': '1845'
    'Cookie': 'JSESSIONID=9B9F10E6DF145905A5663670A91DB3AB'
}

result = requests.get(url, headers=headers)

print(result.text)
