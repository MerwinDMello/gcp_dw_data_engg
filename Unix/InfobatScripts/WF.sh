#############################################################
#  								    #
#   Script Name:    HDW_Workflow.sh              	    #
#                                                           #
#   Description:    This script executes DMExpress Job      #
#   based on input parameter                                # 
#                                                           #
#   Developer  :    Jagjit Natt                             #
#                                                           # 
#############################################################                                                     

set -x
##########################
## Variable Declaration ##
##########################

homedir=/etl/jfmd/EDWHR
export logdate=`date +%Y%m%d_%H_%M_%S`
export JobDate=`date +%Y%m%d_%H:%M:%S`
export Job_Name=$1



#####################################
### Initialize External Variables ###
#####################################

echo "Importing DMX Variables"
. DMX_VARS.sh
echo "Importing Credentials"
. $LOGONDIR/HDW_DB.sh

if [ -f $PARAMFILEDIR/${Job_Name}_prm.sh ]
then
	echo "Parameter file ${Job_Name}_prm.sh exists. Executing..."
	. $PARAMFILEDIR/${Job_Name}_prm.sh
	RC=$?
	if [ $RC -ne 0 ];
	then
		exit 99
	fi
fi

########################################
### Load the table ETL_JOB_RUN Table ###
########################################

. $SCRIPTDIR/ETL_Run_Initial.sh
RC=$?
if [ $RC -ne 0 ];
then
	mail -s "ETL Job Failure - ${Job_Name} - Failed to create ETL Job Run entry" $email < /dev/null
	exit 99
fi


###*************************************###
### Remove temporary files 		     ###
###*************************************###


if [ -s "$TEMPDIR/${Job_Name}_TD_error_code" ]; then rm -f $TMPDIR/${Job_Name}_TD_error_code ;fi
if [ -s "$TMPDIR/${Job_Name}_error_reason" ]; then rm -f $TMPDIR/${Job_Name}_error_reason ;fi
if [ -s "$TMPDIR/${Job_Name}_DMX_error_message" ]; then rm -f $TMPDIR/${Job_Name}_DMX_error_message ;fi
if [ -s "$TMPDIR/${Job_Name}_TD_error_message" ]; then rm -f $$TMPDIR/${Job_Name}_TD_error_message ;fi
if [ -s "$TMPDIR/${Job_Name}_bteq_error_num" ]; then rm -f $TMPDIR/${Job_Name}_bteq_error_num ;fi
if [ -s "$TMPDIR/${Job_Name}_bteq_error_message" ]; then rm -f $TMPDIR/${Job_Name}_bteq_error_message ;fi
if [ -s "$TMPDIR/${Job_Name}_BTEQ_Failure.sql" ]; then rm -f $TMPDIR/${Job_Name}_BTEQ_Failure.sql ;fi




#################################
### Running the DMExpress Job ###
#################################

echo "Starting DMExpress Job"
dmxjob  /run $JOBDIR/$Job_Name/${Job_Name}.dxj  > $LOGDIR/$1.log 2>&1
RC=$?
chmod -f 775 $LOGDIR/$Job_Name.log

case $RC in
0) 
echo "Job ${Job_Name} Successful - ${JobDate}" >> $LOGDIR/HDW_Load_Status.txt
. $SCRIPTDIR/ETL_Run_Success.sh
RC=$?;;
100)
echo "Job ${Job_Name} Completed with Exceptions - ${JobDate}" >> $LOGDIR/HDW_Load_Status.txt
. $SCRIPTDIR/ETL_Run_Success.sh
RC=$?;;
111) 
echo "Job ${Job_Name} Failed - ${JobDate}" >> $LOGDIR/HDW_Load_Status.txt
Status=`grep 'Mismatch Detected. Aborting Job...' $LOGDIR/$1.log`
echo "Status is ${Status}"

if [[ $Status != '' ]]
then 
. $SCRIPTDIR/ETL_Run_Success.sh
else
. $SCRIPTDIR/ETL_Run_Failure.sh
exit 99
fi 

RC=$?;;
esac
if [ $RC -ne 0 ];
then
	mail -s "ETL Job Failure - ${Job_Name} - Failed to update ETL Job Run entry" $email < /dev/null
	exit 99
fi

#########################
### Log File Archival ###
#########################

. $SCRIPTDIR/ETL_Log_Archival.sh
RC=$?
if [ $RC -ne 0 ];
then
	mail -s "ETL Job Failure - ${Job_Name} - Failed to archive the Log file" $email < /dev/null
	exit 99
fi