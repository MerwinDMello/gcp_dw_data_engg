#Script Name	: TMS_Filechecker_Rename.sh
#Description	: This Script checks for the files recieved from SFTP Server on /etl/ST/EDWHR/SrcFiles directory( Placed  By /etl/jfmd/EDWHR/Scripts/TMS_Email.sh)
#Created By 	: Pratap Das
#Created On	: 11.07.2016
#Syntax	        : TMS_Filechecker_Rename.sh {frequency mode and no.of files in the frequency Ex: TMS_Filechecker_Rename.sh DAILY 1 }



#Change History:


#Version No.		Modified By		Modified Date	Comments
############		############		##############	#########

#1.0			Pratap Das		11.07.2016	Created
#1.1                    Syntel                  02.11.2020      Updated to accomidate changes as for JIRA Ticket(HDM-1296)


#########################################################
### Variable Declaration and Parameter Initialization ###
#########################################################
set -x

archiveDate=`date +%F-%T`
LogFile=/etl/ST/EDWHR/LogFiles/TMS_${1}_Filechecker.log
Temp1=/etl/ST/EDWHR/LogFiles/TMS_${1}_Temp.tmp
Temp2=/etl/ST/EDWHR/LogFiles/TMS_${1}_Temp2.tmp
TEMPDIR=/etl/ST/EDWHR/SrcFiles/TMS_${1}_Temp
TEMPDIR_ZIP=/etl/ST/EDWHR/SrcFiles/TMS_${1}_Temp_Zip
SRCDIR=/etl/ST/EDWHR/SrcFiles
rc=0

echo "Script started at `date`">$LogFile



######################################
### Remove the temp file if exists ###
######################################

if [[ $rc -eq 0 ]]
then
   if [ -f $Temp1 ]; then rm -f $Temp1; fi
fi
rc=$?


####################################
### Change Directory to Tempdirectory ###
####################################

if [[ $rc = 0 ]]
then
   ## cd /etl/ST/EDWHR/SrcFiles
   cd ${TEMPDIR}

   rc=$?
   if [[ $rc = 0 ]]
   then
      echo "Directory changed to ${TEMPDIR}.`">>$LogFile
   else
      echo "ERROR in Changing directory to ${TEMPDIR}">>$LogFile
      exit 99
   fi
fi
rc=$?


########################################################
### Check for No. of Files received from SFTP Server ###
########################################################

if [[ $rc = 0 ]]
then
   No_Files_Received=`ls -lrt ??????????????_EDWHR*.csv | wc -l`
   if [[ $No_Files_Received -ne ${2} ]]
   then
      echo " File is not received from the SFTP Server">>$LogFile
      echo "No. of Files received is: $No_Files_Received">>$LogFile
      exit 99
   else
      echo "No. of Files received is: $No_Files_Received">>$LogFile
   fi
fi
rc=$?


################################################################################
### Rename the Files to be used as Source for Staging jobs in Temp directory ###
################################################################################

if [[ $rc = 0 ]]
then
  ls -1 ??????????????_EDWHR*.csv > $Temp1
  
  for filename in `cat $Temp1`
  do
     Curr_File_Name=`echo $filename`
      New_File_Name=`echo $filename | cut -f2- -d"_"`

      if [[ -f $New_File_Name ]]
      then
##       echo "A file with name $New_File_Name already exists in the ${TEMPDIR}. Please move it to archive or delete the file".>>$LogFile
##       exit 99

		echo "A file with name $New_File_Name already exists in the ${TEMPDIR}. Removeing the file".>>$LogFile
		rm -rf  $New_File_Name
		rc=$?
      		   if [[ $rc = 0 ]]
	          then
	               echo "File $New_File_Name remove Successfully.">>$LogFile
       	   else
         		 echo "Error in file $New_File_Name remove.">>$LogFile
	               exit 99
      	   fi
      fi
##	else
        mv $Curr_File_Name $New_File_Name
        rc=$?
         if [[ $rc = 0 ]]
        then
            echo "File $Curr_File_Name is renamed to $New_File_Name Successfully.">>$LogFile
        else
            echo "Error in renaming the file $Curr_File_Name to $New_File_Name.">>$LogFile
            exit 99
        fi
##   fi
   done

ls -1 EDWHR*.csv > $Temp2
cp $Temp2 ${SRCDIR}
fi
cd ${SRCDIR}
rc=$?
########################################################################
### Copy the Files to be used as Source for Staging jobs to SrcDir   ###
########################################################################



if [[ $rc = 0 ]]
then
    for filename in `cat $Temp2`
  do
     Curr_File_Name=`echo $filename`
     ## New_File_Name=`echo $filename | cut -f2- -d"_"`

      if [[ -f $Curr_File_Name ]]
      then
##       echo "A file with name $New_File_Name already exists in the Source file directory. Please move it to archive or delete the file".>>$LogFile
##       exit 99

		echo "A file with name $Curr_File_Name already exists in the Source file directory. Removeing the file".>>$LogFile
		rm -rf  $Curr_File_Name
		rc=$?
      		   if [[ $rc = 0 ]]
	          then
	               echo "File $Curr_File_Name remove Successfully.">>$LogFile
       	   else
         		 echo "Error in file $Curr_File_Name remove.">>$LogFile
	               exit 99
      	   fi
      fi
##	else
        ##mv $Curr_File_Name $New_File_Name
	chmod 755 ${TEMPDIR}/$Curr_File_Name
        cp ${TEMPDIR}/$Curr_File_Name ${SRCDIR}
        rc=$?
         if [[ $rc = 0 ]]
        then
            echo "File $Curr_File_Name is copied to ${SRCDIR} Successfully.">>$LogFile
        else
            echo "Error in copying the file $Curr_File_Name to ${SRCDIR}.">>$LogFile
            exit 99
        fi
##   fi
   done

rm -f ${SRCDIR}/TMS_${1}_Temp2.tmp
cd ${TEMPDIR}
fi
rc=$?
##############################
##############################
### Zip the Original Files ###
##############################

if [[ $rc -eq 0 ]]
then
   for file in `cat $Temp1`
   do
      mv ${TEMPDIR_ZIP}/$file ${TEMPDIR_ZIP}/${file}_${archiveDate}
      gzip ${TEMPDIR_ZIP}/${file}_${archiveDate}

      if [[ $rc -eq 0 ]]
      then
         echo "File $file is zipped Successfully.">>$LogFile
      else
         echo "ERROR in zipping file $file">>$LogFile
         exit 99
      fi
   done
fi
rc=$?

####################################################
### Move the Original Files to Archive directory ###
####################################################

if [[ $rc -eq 0 ]]
then
   mv ${TEMPDIR_ZIP}/*.gz /etl/ARCHIVE/EDWHR/SrcFiles/
   rc=$?
   
   if [[ $rc -eq 0 ]]
   then
      echo "Original Files are archived Successfully.">>$LogFile
       rm -rf ${TEMPDIR_ZIP}
      rc=$?

      if [[ $rc -eq 0 ]]
      then
         echo "Temp directory ${TEMPDIR_ZIP} is removed Successfully.">>$LogFile
      else
         echo "ERROR in removing Temp directory ${TEMPDIR_ZIP}.">>$LogFile  
      fi

   else
      echo "ERROR in archiving the Original Files">>$LogFile
      exit 99
   fi

fi



######################################
### Remove the temp file if exists ###
######################################

if [[ $rc -eq 0 ]]
then
   if [ -f $Temp1 ]; then rm -f $Temp1; fi
   if [ -f $Temp2 ]; then rm -f $Temp2; fi
fi
rc=$?



echo "Script completed at `date`">>$LogFile
exit 0