#!/bin/bash
# ***************************************************************************
# �ļ����ƣ�check_search_log.sh
# �������������search��־������waiting��error�澯
# �� �� ��
# �� �� ��
# �� �� �ߣ�hyn
# �������ڣ�2020325
# �޸���־������ERROR�����ֵ
# �޸����ڣ�2020419
# ***************************************************************************
# ������ø�ʽ��sh check_search_log.sh
# ***************************************************************************

# �������ݿ��¼��Ϣ
EXEC_MYSQL_NG="mysql -h10.97.192.180 -ungtassuite -pAj7y32h! ngtassuite"

# ���Ͷ���
function send_sms()
{
    ${EXEC_MYSQL_NG} -e "INSERT INTO tb_sys_sms_send_cur(serv_number, send_date,text,opt_user,opt_date ) SELECT serv_number, now(), '$1', opt_user, now() FROM tb_sys_sms_phone WHERE opt_user = 'ocetl' and sms_id = '$2';" 2>/dev/null
}

# �����־����
log_line=500

# error��waitting��ؾ���ֵ
monitor_num=5

# ���˷�����
send_who='ALL'
#send_who='HYN'

# ѭ�����65��66����
for i in 65 66
do 
	#echo 1
	# ���65��66 search log
	ssh 10.218.146.$i "tail -$log_line /home/ocdp/ocsearch/server/logs/ocsearch_all.log > /home/ocdp/hyn/ocsearch_log/search_log_$i.txt" 
	
	# ��ȡ��־����ʱ��
	# ssh 10.218.146.$i "tail -1 /home/ocdp/ocsearch/server/logs/ocsearch_all.log | awk '{print $1$2} ' > /home/ocdp/hyn/ocsearch_log/search_log_time_$i.txt" 
	
	#log_time=`ssh 10.218.146.$i "cat /home/ocdp/hyn/ocsearch_log/search_log_time_$i.txt | awk '{print $1,$2}'"`
	#echo $log_time
	#test="ssh 10.218.146.$i "grep waiting /home/ocdp/hyn/search_log/search_log_$i.txt | wc -l"
	
	#echo 2
	waiting_num=`ssh 10.218.146.$i "grep waiting /home/ocdp/hyn/ocsearch_log/search_log_$i.txt | wc -l"`
	#echo 3
	error_num=`ssh 10.218.146.$i "grep ERROR /home/ocdp/hyn/ocsearch_log/search_log_$i.txt | wc -l"`
	#echo 4
	current_time=`date +"%Y-%m-%d %H:%M"`
	
	echo $i waiting������$waiting_num,error������$error_num
	
	# �������waiting��error��Ϣ���͸澯��Ϣ
	if [ $waiting_num -le $monitor_num -a $error_num -le $monitor_num ] ;then
		echo "$i ��־��������,��ǰʱ��Ϊ��$current_time"
		#send_sms "$i ��־��������,��ǰʱ��Ϊ��$current_time" "w01"
	else
		#sms_info="$i search��־����$log_line����$waiting_num��waiting��$error_num��ERROR����,��鿴����ǰʱ��Ϊ��$current_time"
		sms_info="$i����search��־����$log_line����$waiting_num��waiting��$error_num��ERROR����,��鿴����ǰʱ��Ϊ��$current_time"
		echo $sms_info
		send_sms "$i����search��־����$log_line����$waiting_num��waiting��$error_num��ERROR����,��鿴����ǰʱ��Ϊ��$current_time" $send_who
		#send_sms "$i����search��־����$log_line����$waiting_num��waiting��$error_num��ERROR����,��鿴����ǰʱ��Ϊ��$current_time" "ALL"
		#send_sms "����ocsearch" "HYN"
	fi
	
done



