#!/bin/bash

#bringing parameter file
. /home/saif/project_1/env/sqp_param.prm
. /home/saif/project_1/env/mysql_param.prm


mv /home/saif/project_1/datasets/Day*.csv /home/saif/project_1/datasets/Day.csv


#Creating parameters for logging file
#PASSWD=`sh password.sh`
LOG_DIR=/home/saif/project_1/logs/
DT=`date '+%Y-%m-%d %H:%M:%S'`
DT1=`date '+%Y%m%d'`
LOG_FILE=${LOG_DIR}/project_1_daily.log


#Running MySQL commands
mysql --local-infile=1 -uroot -p${SQL_PASSWD} < /home/saif/project_1/scripts/sql_daily.txt


#Logging the success and failure of SQL commands
if [ $? -eq 0 ]
then echo "sql insertion  and updation successfully executed at ${DT}" >> ${LOG_FILE}
else echo "sql commands failed  at ${DT} " >> ${LOG_FILE}
exit 1
fi


#Executing the sqoop import job
sqoop job --exec pro_job_imp


#Logging the sqoop import status
if [ $? -eq 0 ]
then echo "sqoop imp job successfully executed at ${DT}" >> ${LOG_FILE}
else echo "sqoop imp job failed  at ${DT} " >> ${LOG_FILE}
exit 1
fi


#Executing the HIVE commands
hive -f /home/saif/project_1/scripts/hive_daily.hql



#Logging the status of HVE commands execution
if [ $? -eq 0 ]
then echo "hive scd successfully executed at ${DT}" >> ${LOG_FILE}
else echo "hive scd job failed  at ${DT} " >> ${LOG_FILE}
exit 1
fi


#Truncating MySQL day_recol table
mysql --local-infile=1 -uroot -p${SQL_PASSWD} < /home/saif/project_1/scripts/sql_dayrecol_trun.txt


#Logging the trauncate status
if [ $? -eq 0 ]
then echo "MySQL day_recol table successfully truncated at ${DT}" >> ${LOG_FILE}
else echo "MySQL day_recol table truncation failed!!! at ${DT} " >> ${LOG_FILE}
exit 1
fi

#Executing the SQOOP export job
sqoop job --exec pro_job_exp


#Logging the SQOOP export status
if [ $? -eq 0 ]
then echo "sqoop exp job successfully executed at ${DT}" >> ${LOG_FILE}
else echo "sqoop exp job failed  at ${DT} " >> ${LOG_FILE}
exit 1
fi


#Executing MySQL commands for checking the records for reconciliation
mysql --local-infile=1 -uroot -p${PASSWD} < /home/saif/project_1/scripts/sql_cmp.txt


#Logging the status above SQL commands
if [ $? -eq 0 ]
then echo "sql comparison successfully executed at ${DT}" >> ${LOG_FILE}
else echo "sql comparison failed  at ${DT} " >> ${LOG_FILE}
exit 1
fi


#Moving the dataset to archives directory
mv /home/saif/project_1/datasets/Day*.csv /home/saif/project_1/archives/Day_${DT1}.csv


#Logging the status of dataset archival
if [ $? -eq 0 ]
then echo "successfully archived at ${DT}" >> ${LOG_FILE}
else echo "archival failed  at ${DT} " >> ${LOG_FILE}
exit 1
fi


echo "******************************************************************************************************" >> ${LOG_FILE}
