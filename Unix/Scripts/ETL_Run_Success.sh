echo "DMX Job Successful. Updating ETL Job Run Entry"

bteq << EOF >> /dev/null

.SET ERROROUT STDOUT
.RUN FILE $LOGONDIR/HDW_AC
UPDATE $NCR_AC_VIEW.etl_job_run SET Job_End_Date_Time = CURRENT_TIMESTAMP(0), Job_Status_Code = 0 WHERE Job_End_Date_Time IS NULL AND job_name = '$Job_Name' AND Job_Start_Date_Time=(select MAX(Job_Start_Date_Time) from $NCR_AC_VIEW.Etl_Job_Run where job_name='$Job_Name');
.LOGOFF
.QUIT

EOF