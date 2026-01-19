#Script Name	: TMS_Email.sh
#Description	: This Script checks the files received from SFTP server to TMS_${1}_TMP directory and and Sends Email accordingly.
#		  It also copies the files to /etl/ST/EDWHR/ScrFiles directory.
#Created By 	: Pratap Das
#Created On	: 11.07.2016
#Syntax	        : TMS_Email.sh {frequency mode Ex: TMS_Email.sh DAILY}



#Change History:


#Version No.		Modified By		Modified Date		Comments
############		############		##############	#########

#1.0			Pratap Das		11.07.2016		Created
#1.1			Syntel		        02.11.2020		Updated as per JIRA Ticket (HDM-1296)




#########################################################
### Variable Declaration and parameter initialization ###
#########################################################


set -x
LOGDIR=/etl/ST/EDWHR/LogFiles
TMPDR=/etl/ST/EDWHR/SrcFiles/TMS_${1}_Temp
SrcDir=/etl/ST/EDWHR/SrcFiles
LOGFILE=$LOGDIR/TMS_${1}_Email.log
Notify_To="CorpIS.DLPDESClinicalEntRptgAlert@HCAHealthcare.com"
Notify_Cc="CorpIS.DLPDESClinicalEntRptgAlert@HCAHealthcare.com"
##Notify_To="China.Thirupalu@hcahealthcare.com"
##Notify_Cc="China.Thirupalu@hcahealthcare.com"
rc=0


echo "Script started at `date`" >$LOGFILE

######################################
### Remove the temp files if exist ###
######################################

if [[ $rc -eq 0 ]]
then
   if [ -f ${SrcDir}/TMS_${1}_File_Status.txt ]; then rm -f ${SrcDir}/TMS_${1}_File_Status.txt; fi
   if [ -f ${SrcDir}/TMS_${1}_File.lst ]; then rm -f ${SrcDir}/TMS_${1}_File.lst; fi
   if [ -f ${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt ]; then rm -f ${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt; fi
   if [ -f $LOGFILE ]; then rm -f $LOGFILE; fi

fi
rc=$?


####################################
### Change directory to ${TMPDR} ###
####################################
if [[ $rc -eq 0 ]]
then
   echo "Temp files of previous run are removed." >>$LOGFILE
   cd ${TMPDR}
   rc=$?

   if [[ $rc -eq 0 ]]
   then
      echo "Directory changed to ${TMPDR} successfully." >>$LOGFILE
   else
      echo "ERROR in changing directory to ${TMPDR}." >>$LOGFILE
      echo "Script failed at `date`" >>$LOGFILE
      exit 99
   fi

else
   echo "ERROR in removing Temp files of previous run." >>$LOGFILE
   echo "Script failed at `date`" >>$LOGFILE
   exit 99
fi
rc=$?




############################################################
### check for Files received on TEMP directory from SFTP ###
### and make entry into TMS_${1}_File.lst                ###
############################################################

if [[ $(find ${TMPDR} -name "??????????????_EDWHR_*.csv"|wc -l|sed 's/ //g') -eq 0 ]]
then
   touch ${SrcDir}/TMS_${1}_File.lst
else
   ls ??????????????_EDWHR_*.csv > ${SrcDir}/TMS_${1}_File.lst
  ## cp ??????????????_EDWHR_*.csv ${SrcDir}

   rc=$?
   if [[ $rc -eq 0 ]]
   then
      echo "Source File names are copied to TMS_${1}_File.lst Succesfully." >>$LOGFILE
   else
      echo "Error in Copying the Source File names to TMS_${1}_File.lst." >>$LOGFILE
      echo "Script failed at `date`" >>$LOGFILE
      exit 99
   fi
fi
rc=$?



#####################################
### Change directory to ${SrcDir} ###
#####################################

if [[ $rc -eq 0 ]]
then
   cd ${SrcDir}
   rc=$?
   if [[ $rc -eq 0 ]]
   then
      echo "Directory changed to ${SrcDir} successfully." >>$LOGFILE
   else
      echo "ERROR in changing directory to ${SrcDir}." >>$LOGFILE
      echo "Script failed at `date`" >>$LOGFILE
      exit 99
   fi
fi



##################
### Send Email ###
##################



if [[ -s ${SrcDir}/TMS_${1}_File.lst ]]
then
   echo "Hi Team, "> ${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt
   echo "\n">> ${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt
   echo "Below are the csv files that will be processed on date `date` ">> ${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt
   echo "">>${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt
   cat ${SrcDir}/TMS_${1}_File.lst >> ${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt
   echo "">>${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt
   echo "">>${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt
   echo "">>${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt
   echo "Thanks,">>${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt
   echo "HDM Team">>${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt
	
   mail -s "${1} Files to be Processed for TMS" -c ${Notify_Cc} ${Notify_To} < ${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt
else
   echo "Hi Team, "> ${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt
   echo "\n">> ${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt
   echo "No TMS files are received from SFTP for Processing on `date`">> ${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt
   echo "">>${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt
   echo "">>${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt
   echo "">>${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt
   echo "Thanks,">>${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt
   echo "HDM Team">>${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt

   mail -s "${1} Files to be Processed for TMS" -c ${Notify_Cc} ${Notify_To} < ${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt
   echo "Script failed at `date`" >>$LOGFILE
   exit 99
fi
rc=$?



############################################
### Remove the temp files of current run ###
############################################
if [[ $rc -eq 0 ]]
then
   if [ -f ${SrcDir}/TMS_${1}_File_Status.txt ]; then rm -f ${SrcDir}/TMS_${1}_File_Status.txt; fi
#   if [ -f ${SrcDir}/TMS_${1}_File.lst ]; then rm -f ${SrcDir}/TMS_${1}_File.lst; fi
   if [ -f ${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt ]; then rm -f ${SrcDir}/TMS_${1}_ProcFile_Mail_Body.txt; fi
else
   echo "Script failed at `date`" >>$LOGFILE
   exit 99
fi


echo "Script completed at `date`" >>$LOGFILE