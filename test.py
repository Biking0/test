# encoding=utf8
# Scheduled to run on Friday
# by hyn
# 20200220

# import requests
#
# url = ""
# headers = {
#     'Authorization': "admin:admin",
#
# }
#
# print (123)

import time
import datetime

# print  time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
# print  time.strftime('%Y-%m-%d',time.localtime(time.time()))
# today_str=time.strftime('%Y-%m-%d', time.localtime(time.time()))
#
# print  time.strptime(today_str+' 15:00:00', '%Y-%m-%d %H:%M:%S')

# 获取周五当天15点时间
today_str = time.strftime('%Y-%m-%d', time.localtime(time.time()))
stamptime_15 = time.mktime(time.strptime(today_str + ' 15:00:00', '%Y-%m-%d %H:%M:%S'))

# 获取当前时间戳
stamptime_now=time.time()

# 是否到当天15点以后
#print stamptime_15
#print stamptime_now


