#Script Name	: Del_TMS_Files.sh
#Description	: This Script creates a script "/etl/jfmd/EDWHR/Scripts/TMS_SFTP_${1}_Del_New.sh" in run time to removes the received TMS files from the SFTP Server.
#Created By 	: Pratap Das
#Created On	: 11.07.2016
#Syntax	: Del_TMS_Files.sh {No Parameter is required.}



#Change History:


#Version No.		Modified By		Modified Date  Comments
############		############		##############	#########

#1.0			Pratap Das		11.07.2016	Created
#1.1                    Syntel                  02.11.2020      Updated to accomidate changes as for JIRA Ticket(HDM-1296)

#########################################################
### Variable Declaration and parameter initialization ###
#########################################################
set -x
LOGDIR=/etl/ST/EDWHR/LogFiles
SrcDir=/etl/ST/EDWHR/SrcFiles
LOGFILE=$LOGDIR/Del_TMS_${1}_Files.log
TEMPDIR=/etl/ST/EDWHR/SrcFiles/TMS_${1}_Temp


File_List=/etl/ST/EDWHR/SrcFiles/TMS_${1}_File.lst
rc=0


echo "Script started at `date`" >$LOGFILE


if [[ -s ${SrcDir}/TMS_${1}_File.lst ]]
then
   echo "${SrcDir}/TMS_${1}_File.lst file exists with data">>$LOGFILE
   head -15 /etl/jfmd/EDWHR/Scripts/TMS_SFTP_Del.sh > /etl/jfmd/EDWHR/Scripts/TMS_SFTP_${1}_Del_New.sh
   for file in `cat $File_List`
   do 
      echo  "send \"rm ${file} \\\n\"">> /etl/jfmd/EDWHR/Scripts/TMS_SFTP_${1}_Del_New.sh
      tail -2 /etl/jfmd/EDWHR/Scripts/TMS_SFTP_Del.sh |head -1 >> /etl/jfmd/EDWHR/Scripts/TMS_SFTP_${1}_Del_New.sh
   done
   tail -1  /etl/jfmd/EDWHR/Scripts/TMS_SFTP_Del.sh >> /etl/jfmd/EDWHR/Scripts/TMS_SFTP_${1}_Del_New.sh
   echo "/etl/jfmd/EDWHR/Scripts/TMS_SFTP_${1}_Del_New.sh is cretaed">>$LOGFILE

   chmod 777 /etl/jfmd/EDWHR/Scripts/TMS_SFTP_${1}_Del_New.sh
   rc=$?
   if [[ $rc = 0 ]]
   then
      echo "Permissions are granted on /etl/jfmd/EDWHR/Scripts/TMS_SFTP_${1}_Del_New.sh">>$LOGFILE
   else
      echo "ERROR in granting Permissions on /etl/jfmd/EDWHR/Scripts/TMS_SFTP_${1}_Del_New.sh">>$LOGFILE
   fi
   
else
   echo "${SrcDir}/TMS_${1}_File.lst file doesn't have any data">>$LOGFILE
   echo "echo 'There is no files to remove from SFTP location' " >/etl/jfmd/EDWHR/Scripts/TMS_SFTP_${1}_Del_New.sh
   echo "echo 'There is no files to remove from SFTP location' " >>$LOGFILE
fi

echo "Script completed at `date`" >>$LOGFILE