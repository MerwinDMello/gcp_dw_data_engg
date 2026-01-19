set -x
TD_Error_Check(){
echo "TD_Error_Check"
tdl=`grep "$DMXTeradataDirectory" ${LOGDIR}/${Job_Name}.log | tail -1 | tr ' ' '\n' | grep "$DMXTeradataDirectory" | tr -d '\"'`
if [ $tdl -eq '']
then 
	return 1
fi
td_error_code=`grep "Highest return code encountered" $tdl | cut -d"'" -f2`
echo "td_error_code"
if [ $td_error_code -eq 0 ]
then
	return 1
fi
eline=`grep -nv "^ " $tdl | awk '/fail/ { getline; print $0 }' | cut -d":" -f1`
sline=`grep -nv "^ " $tdl | grep -i "fail" | cut -d":" -f1`
eline=$((eline-1))
eline_as=`sed -n "${sline},${eline}p" $tdl | grep -n "\." | cut -d":" -f1`
sed -n "${sline},${eline}p" $tdl | head -${eline_as} | sed 's/     / /g' | tr -d '\n' > $TEMPDIR/${Job_Name}_TD_error_message
echo $td_error_code > $TEMPDIR/${Job_Name}_TD_error_code
echo "3|td_util" > $TEMPDIR/${Job_Name}_error_reason
}

DMX_Error_Check(){
echo "DMX_Error_Check"
grep -i "TDTERROR"  ${LOGDIR}/${Job_Name}.log | sed 's/^ *//g'|sed -e 's/[;,()'\'']/ /g;s/  */ /g' > $TEMPDIR/${Job_Name}_DMX_error_message
grep -i "error"  ${LOGDIR}/${Job_Name}.log | sed 's/^ *//g'|sed -e 's/[;,()'\'']/ /g;s/  */ /g' >> $TEMPDIR/${Job_Name}_DMX_error_message
if [ ! -s $TEMPDIR/${Job_Name}_DMX_error_message ]
then
	return 1
fi
echo "4|dmx" > $TEMPDIR/${Job_Name}_error_reason
}

if [ ! -s $TEMPDIR/${Job_Name}_error_reason ]
then
	echo $TEMPDIR
	TD_Error_Check
	if [ $? -ne 1 ]
	then
		error_code=`cat $TEMPDIR/${Job_Name}_TD_error_code`
		error_message=`cat $TEMPDIR/${Job_Name}_TD_error_message`
	fi
	DMX_Error_Check
	if [ $? -ne 1 ]
	then
		error_code='9999992'
		error_message=`cat $TEMPDIR/${Job_Name}_DMX_error_message`
	fi
	
	insert_statement="INSERT INTO PROD_ERROR_DB.ETL_Job_Failure SELECT '${Job_Name}', MAX(Job_Start_Date_Time),Current_Timestamp(0),"${error_code}",'${error_message}',NULL,NULL,NULL  from  $NCR_AC_VIEW.etl_job_run  where job_name ='${Job_Name}' and job_end_date_time IS NULL ;"
	logoff=".LOGOFF"
	logon=".run file $LOGONDIR/HDW_AC"
	echo $logon > $TEMPDIR/${Job_Name}_BTEQ_Failure.sql
	echo $insert_statement >> $TEMPDIR/${Job_Name}_BTEQ_Failure.sql
	echo $logoff >> $TEMPDIR/${Job_Name}_BTEQ_Failure.sql
	echo ".QUIT" >> $TEMPDIR/${Job_Name}_BTEQ_Failure.sql

	#  Bteq call to run the insert query ###





	bteq < $TEMPDIR/${Job_Name}_BTEQ_Failure.sql >>/dev/null


	RC=$?
	if [ $RC -ne 0 ];
	then
		mail -s "$emailsubject" $error_notify < $TEMPDIR/${Job_Name}_error_reason
#		rm -f $TEMPDIR/${Job_Name}_error_reason
		
	fi
fi





echo "DMX Job Failed. Updating ETL Job Run Entry"

bteq << EOF >> /dev/null

.SET ERROROUT STDOUT
.RUN FILE $LOGONDIR/HDW_AC
UPDATE $NCR_AC_VIEW.etl_job_run SET Job_End_Date_Time = CURRENT_TIMESTAMP(0), Job_Status_Code = 99 WHERE Job_End_Date_Time IS NULL AND job_name = '$Job_Name' AND Job_Start_Date_Time=(select MAX(Job_Start_Date_Time) from $NCR_AC_VIEW.Etl_Job_Run where job_name='$Job_Name');
.LOGOFF
.QUIT

EOF