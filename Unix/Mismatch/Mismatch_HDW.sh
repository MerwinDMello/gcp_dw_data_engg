echo "Checking for Mismatch"
logon=".run file /etl/ST/EDWHR/LOGON/HDW_AC"
logoff=".logoff"


   if [ -f $TEMPDIR/${Job_Name}_mismatch.txt ]; then rm -f $TEMPDIR/${Job_Name}_mismatch.txt; fi
   if [ -f $TEMPDIR/${Job_Name}_mismatch_result.sql ]; then rm -f $TEMPDIR/${Job_Name}_mismatch_result.sql; fi
   if [ -f $TEMPDIR/${Job_Name}_mismatch_result.txt ]; then rm -f $TEMPDIR/${Job_Name}_mismatch_result.txt; fi



###bteq << EOF > $TEMPDIR/${Job_Name}_mismatch.txt
###.SET ERROROUT STDOUT
###.RUN FILE $LOGONDIR/HDW_AC
###SELECT CASE sum(abs(b.control_value_expected - a.control_value_actual)) when	0 then 'SUCCESS' else 'FAIL' end as MISMATCH 
###from $NCR_AC_SCHEMA.etl_control_actual a join $NCR_AC_SCHEMA.etl_control_expected b
###on a.job_name = b.job_name and a.control_id = b.control_id and	a.job_start_date_time = b.job_start_date_time 
###join (SELECT max(job_start_date_time) as job_start_date_time from $NCR_AC_SCHEMA.etl_job_run where job_name ='$Job_Name') jr
###on a.job_start_date_time= jr.job_start_date_time
###where a.job_name ='$Job_Name';
###.LOGOFF
###.QUIT
###
###EOF

bteq << EOF > $TEMPDIR/${Job_Name}_mismatch.txt
.SET ERROROUT STDOUT
.RUN FILE $LOGONDIR/HDW_AC
SELECT
CASE WHEN COALESCE( SUM( CASE WHEN A.CONTROL_VALUE_ACTUAL/10000  BETWEEN 
CAST(B.CONTROL_VALUE_EXPECTED/10000 - ((B.CONTROL_VALUE_EXPECTED/10000)*(B.CONTROL_TOLERANCE_PERCENT) /100) AS DECIMAL(18,3))
AND CAST(B.CONTROL_VALUE_EXPECTED/10000 + ((B.CONTROL_VALUE_EXPECTED/10000)*(B.CONTROL_TOLERANCE_PERCENT) /100) AS DECIMAL(18,3)) 
THEN 0 ELSE 1 END ) ,0) =0 THEN 'SUCCESS' ELSE 'FAIL' END AS MISMATCH 
from $NCR_AC_VIEW.etl_control_actual a join $NCR_AC_VIEW.etl_control_expected b
on a.job_name = b.job_name and a.control_id = b.control_id and	a.job_start_date_time = b.job_start_date_time 
join (SELECT max(job_start_date_time) as job_start_date_time from $NCR_AC_VIEW.etl_job_run where job_name ='$Job_Name') jr
on a.job_start_date_time= jr.job_start_date_time
where a.job_name ='$Job_Name';
.LOGOFF
.QUIT

EOF


status=`grep ^"SUCCESS" $TEMPDIR/${Job_Name}_mismatch.txt`

if [ -z "$status" ];
then


   echo "Mismatch Detected. Aborting Job..."


echo $logon >> $TEMPDIR/${Job_Name}_mismatch_result.sql
echo ".set width 65531" >> $TEMPDIR/${Job_Name}_mismatch_result.sql
echo ".export report file=$TEMPDIR/${Job_Name}_mismatch_result.txt;" >> $TEMPDIR/${Job_Name}_mismatch_result.sql
echo "select
EX.Job_Name,
EX.Job_Start_Date_Time,
EX.Control_ID as Control_ID_EXP, 
EX.Control_Value_Expected,
AC.control_ID as Control_ID_ACT,
AC.Control_Value_Actual
from $NCR_AC_VIEW.etl_control_actual AC
join $NCR_AC_VIEW.etl_control_expected EX
        on EX.job_name = AC.job_name
        and EX.control_id = AC.control_id
        and EX.job_start_date_time = (select max(job_start_date_time) from $NCR_AC_VIEW.etl_control_actual where job_name = '$Job_Name')
        and AC.job_start_date_time = (select max(job_start_date_time) from $NCR_AC_VIEW.etl_control_expected where job_name  ='$Job_Name')
        and EX.job_start_date_time = AC.job_start_date_time;" >> $TEMPDIR/${Job_Name}_mismatch_result.sql
echo ".export reset;" >> $TEMPDIR/${Job_Name}_mismatch_result.sql
insert_statement="INSERT INTO prod_error_db.ETL_Job_Failure SELECT '${Job_Name}', MAX(Job_Start_Date_Time),Current_Timestamp(0),'9,999,991','Job Failed with Mismatch',NULL,NULL,NULL  from $NCR_AC_SCHEMA.etl_job_run  where job_name ='${Job_Name}' and job_end_date_time IS NULL ;"
echo $insert_statement >> $TEMPDIR/${Job_Name}_mismatch_result.sql

echo $logoff >> $TEMPDIR/${Job_Name}_mismatch_result.sql
echo ".QUIT;" >> $TEMPDIR/${Job_Name}_mismatch_result.sql


bteq < $TEMPDIR/${Job_Name}_mismatch_result.sql


   echo "Mismatch Detected. Aborting Job..."
   mail -s "HR Data Mart Job $Job_Name failed due to Mismatch" $error_notify <  $TEMPDIR/${Job_Name}_mismatch_result.txt
else
   echo "No Mismatch Detected"
   if [ -f $TEMPDIR/${Job_Name}_mismatch.txt ]; then rm -f $TEMPDIR/${Job_Name}_mismatch.txt; fi
   if [ -f $TEMPDIR/${Job_Name}_mismatch_result.sql ]; then rm -f $TEMPDIR/${Job_Name}_mismatch_result.sql; fi
   if [ -f $TEMPDIR/${Job_Name}_mismatch_result.txt ]; then rm -f $TEMPDIR/${Job_Name}_mismatch_result.txt; fi
fi



