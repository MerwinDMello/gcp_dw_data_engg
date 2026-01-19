
PM_HOME='/etl/jfmd'
bteqfilename=$1
wrklogfile=`echo $bteqfilename | cut -d"." -f1`'_w.log'
errlogfile=`echo $bteqfilename | cut -d"." -f1`'_e.log'
logfilepath=$TEMPDIR/$wrklogfile
errlogfilepath=$TEMPDIR/$errlogfile
emailsubject='${Job_Name} - BTEQ Failure - ${bteqfilename}'

#####################################
#    BTEQ Error Capture Function    #
#####################################

BTEQ_Error_Check(){
bteq_error_num=`sed "s/[^0-9]//g" $errlogfilepath  | grep -v "^$" | head -1 | sed 's/ //g'`
if [ "$bteq_error_num" -eq "" ]
then
	return $bteq_error_num
fi
grep -v 'EOF on RUN file' $errlogfilepath | grep -v "^$" | sed 's/\*\*\* //' | sed 's/ //' | grep -v "^[[:space:]]" | tr -d "\n" | tr -d "'" > $TEMPDIR/${Job_Name}_bteq_error_message
echo "" >> $TEMPDIR/${Job_Name}_bteq_error_message
echo $bteq_error_num > $TEMPDIR/${Job_Name}_bteq_error_num
echo "1|bteq" > $TEMPDIR/${Job_Name}_error_reason
}

if [ -s ${logfilepath} ]; then rm -f ${logfilepath} ;fi
if [ -s ${errlogfilepath} ]; then rm -f ${errlogfilepath} ;fi

########################
#    BTEQ Execution    #
########################

. ${SCRIPTDIR}/${bteqfilename} ${logfilepath} 2> ${errlogfilepath}
RC=$?

cat $errlogfilepath
cat $errlogfilepath >> $logfilepath

##########################
#    BTEQ Error Check    #
##########################

if [ $RC -ne 0 ];
then 
	> $TEMPDIR/${Job_Name}_error_reason
	BTEQ_Error_Check
	bteq_error_num=`cat $TEMPDIR/${Job_Name}_bteq_error_num`
	bteq_error_message=`cat $TEMPDIR/${Job_Name}_bteq_error_message`
	insert_statement="INSERT INTO PROD_ERROR_DB.ETL_Job_Failure SELECT '${Job_Name}', MAX(Job_Start_Date_Time),Current_Timestamp(0),'${bteq_error_num}','${bteq_error_message}',NULL,NULL,NULL  from  $NCR_AC_VIEW.etl_job_run  where job_name ='${Job_Name}' and job_end_date_time IS NULL ;"
	logoff=".LOGOFF"
	logon=".run file $LOGONDIR/HDW_AC"
	echo $logon > $TEMPDIR/${Job_Name}_BTEQ_Failure.sql
	echo $insert_statement >> $TEMPDIR/${Job_Name}_BTEQ_Failure.sql
	echo $logoff >> $TEMPDIR/${Job_Name}_BTEQ_Failure.sql
	echo ".QUIT" >> $TEMPDIR/${Job_Name}_BTEQ_Failure.sql

	##################################
	#    Load error info to table    #
	##################################

	bteq < $TEMPDIR/${Job_Name}_BTEQ_Failure.sql >>/dev/null


	RC=$?
	if [ $RC -ne 0 ];
	then
		mail -s "$emailsubject" $email < $logfilepath
		rm -f $logfilepath
		exit 99
	fi
	exit 99
fi

echo `date`' - Step 2 Execute BTEQ - Succeeded\n' >> $logfilepath