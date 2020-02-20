# python test.py `curl http://www.weather.com.cn/data/sk/101010100.html`

# help
# python json_format.py json_text
import os
import sys
import json

length = len(sys.argv)

print "=================================="
if length > 1:
    try:
        jsonstr = sys.argv[1]
        jsonObj = json.loads(jsonstr)
        formatJsonStr = json.dumps(jsonObj, indent=4, ensure_ascii=False, sort_keys=True)

        print formatJsonStr
        print type(formatJsonStr)
        print jsonObj
        print type(jsonObj)

        print jsonObj['weatherinfo']['WD']

    except Exception, e:
        # print e
        print "json parse error."
else:
    print "argv's length is 1, no json text input."
print "=================================="