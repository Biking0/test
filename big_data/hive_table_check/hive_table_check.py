# encoding=utf-8
#********************************************************************************
# �ļ����ƣ�hive_table_check.py
# ������������Ϊhive���ӳټ�أ���ط���
# �� �� ��
# �� �� ��
# �� �� �ߣ�hyn
# �������ڣ�20200220
# �޸���־��20200420
# �޸����ڣ�
# ****************************************************************************
# ������ø�ʽ��nohup python hive_table_check.py >> nohup.out &
# *******************************************************************************

# encoding=utf-8
# hive_table_check
# by hyn
# 20200224

import os
import time
import datetime
import config

input_data='202002'

day_hour=['01','02','03','04','05','06','07','08',
                    '09','10','11','12','13','14','15','16',
                    '17','18','19','20','21','22','23','24',]

def main():
    for i in config.hour_table:
        
        month_str=datetime.datetime.now().strftime('%Y%m')
        day_str=datetime.datetime.now().strftime('%Y%m%d')
        
        
        #hour_str=datetime.datetime.now().strftime('%Y%m%d%H')
        hour_str=(datetime.datetime.now()-datetime.timedelta(hours=i[1])).strftime('%Y%m%d%H')
        
        
        
        table_name = i[0]
        print table_name
        table_info_sh=config.bdi_hadoop_path+table_name+'/month_id='+month_str+'/day_id='+day_str+'/hour_id='+hour_str+'/ > table_info.txt'
        print table_info_sh
        
        os.popen(table_info_sh)
        
        
        f=open('table_info.txt').readlines()
        print f
        print type(f)
        # ����������
        if len(f) == 0 :
			
			#�澯ʱ��
			now_time=' �澯ʱ�䣺'+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
            print 'bdiƽ̨�������ӳ٣�',i[2]
			
			sms_info='bdiƽ̨�������ӳ٣�'+i[2])+now_time
            
            send_chk_sh='sh wh_create_chk.sh yiqing 20200109 '
            
            print send_chk_sh
            
            os.popen(send_chk_sh).readlines()
            continue
            
        print f[0].split(' ')
        result=f[0].split(' ')
		
		# ����Ϊ��
        if not result[1]=='G':
            print 'bdiƽ̨��������Ϊ��',i[2]
            
            send_chk_sh='sh wh_create_chk.sh yiqing 20200109 '
            
            print send_chk_sh
            
            os.popen(send_chk_sh).readlines()
            continue
        print '����Сʱ������������'



#demo()
            
            
        
        
