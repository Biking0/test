# -*-coding:utf-8 -*-
#********************************************************************************
# �ļ����ƣ�run_hive_table_check.py
# ������������Ϊ������
# �� �� ��
# �� �� ��
# �� �� �ߣ�hyn
# �������ڣ�20191020
# �޸���־��20191226
# �޸����ڣ�
# ****************************************************************************
# ������ø�ʽ��nohup python run_hive_table_check.py >> nohup.out &
# *******************************************************************************

import os
import sys
import time
import hive_table_check

# ÿ���Сʱ�澯һ��
sleep_time=1800

# ����
if __name__=='__main__':
  
  while True:       

		hive_table_check.main()
	   
		# ��Ϣ10���ӣ�600
		print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()),'sleep '+str(sleep_time)+'s'
		time.sleep(sleep_time)
