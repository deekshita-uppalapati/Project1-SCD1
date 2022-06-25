#!/bin/bash


#Sourcing the parameter file
. /home/saif/project_1/env/sqp_param.prm
. /home/saif/project_1/env/mysql_param.prm


#Creating parameters for log files
LOG_DIR=/home/saif/project_1/logs
DT=`date '+%Y-%m-%d %H:%M:%S'`
LOG_FILE=${LOG_DIR}/project_1_ots.log


#Creating tables in MySQL
mysql --local-infile=1 -uroot -p${SQL_PASSWD} < /home/saif/project_1/scripts/sql_ots.txt


#Logging for MySQL commands execution Success
if [ $? -eq 0 ]
then echo "sql successfully executed at ${DT}" >> ${LOG_FILE}
else echo "sql commands failed  at ${DT} " >> ${LOG_FILE}
exit 1
fi


#Sqoop Job Creation for Importing Data from MySQL to HDFS
sqoop job --create pro_job_imp -- import \
--connect jdbc:mysql://${LOCALHOST}:${PORT_NO}/${DB_NAME}?useSSL=False \
--username ${USERNAME} --password-file ${PASSWORD_FILE} \
--query 'SELECT custid, username, quote_count, ip, entry_time, prp_1, prp_2, prp_3, ms, http_type, purchase_category, total_count, purchase_sub_category, http_info, status_code,year_col,month_col FROM day WHERE $CONDITIONS' -m 1 \
--delete-target-dir \
--target-dir ${OP_DIR}


#Logging for data ingestion succes and failure
if [ $? -eq 0 ]
then echo "sqoop imp job successfully created at ${DT}" >> ${LOG_FILE}
else echo "sqoop imp job  failed  at ${DT} " >> ${LOG_FILE}
exit 1
fi


#Executing HIVE commands for creating hive tables
hive -f /home/saif/project_1/scripts/hive_ots.hql


#Logging the HIVE commands execution success or failure
if [ $? -eq 0 ]
then echo "hive ots successfully executed at ${DT}" >> ${LOG_FILE}
else echo "sqoop hive ots  failed  at ${DT} " >> ${LOG_FILE}
exit 1
fi


#Sqoop job creation for exporting data from HDFS to MySQL
sqoop job --create pro_job_exp -- export \
--connect jdbc:mysql://localhost:3306/project_1?useSSL=False \
--table day_recol \
--columns custid,username,quote_count,ip,entry_time,prp_1,prp_2,prp_3,ms,http_type,purchase_category,total_count,purchase_sub_category,http_info,status_code,year_col,month_col \
--username root --password Welcome@123 \
--direct \
--export-dir /user/hive/warehouse/project_1.db/temp2/ \
--m 1 \
-- driver com.mysql.jdbc.Driver --input-fields-terminated-by ','


#Logging the export success or failure
if [ $? -eq 0 ]
then echo "sqoop exp job successfully created at ${DT}" >> ${LOG_FILE}
else echo "sqoop exp job  failed  at ${DT} " >> ${LOG_FILE}
exit 1
fi
