#!/bin/bash
####################################################################################
#Script Name	: HDW_TMS_SrcFiles_Archive.sh                                      #
#Description	: This script archives the source file.                            #
#Created By 	: Skylar Youngblood                                                #
#Created On	: 08/26/2018                                                       #
#Syntax	: HDW_TMS_SrcFiles_Archive.sh <FileName_????????.txt>                      #
#                                                                                  #
#                                                                                  #
####################################################################################
# Change Control:                                               #
#                                                               #
# Date                                     INITIAL RELEASE      #
# 08/26/2018 Skylar Youngblood              Initial version     #
#################################################################

#########################################################
### Variable Declaration and Parameter Initialization ###
#########################################################

export FileName=$1
Date_Part=`date +%Y%m%d`
export TMS_SrcFiles=/etl/ST/EDWHR/SrcFiles
export ARCHIVEDIR=/etl/ARCHIVE/EDWHR/SrcFiles
export LogFile=/etl/ST/EDWHR/LogFiles/HDW_TMS_SrcFiles_Archive_${FileName}.log

RC=0

echo "Script Started at `date`">$LogFile

#######################
### Parameter Check ###
#######################


if [[ $# -ne 1 ]]
   then
	echo "No of parameters passed are incorrect. Please pass atleast one parameter" >>$LogFile
       exit 99
fi
RC=$?



############################
### Compress the SrcFile ###
############################

if [[ $RC = 0 ]]
then
   gzip ${TMS_SrcFiles}/${FileName}

   RC=$?
   
   if [[ $RC = 0 ]]
   then
      echo "Source File ${FileName} is compressed Successfully.">>$LogFile
   else
      echo "ERROR: Error in compressing the Source File ${FileName}.">>$LogFile
      exit 99
   fi

fi
rc=$?

###################################
### Archive the Compressed File ###
###################################

if [[ $RC = 0 ]]
then
   mv ${TMS_SrcFiles}/${FileName}.gz ${ARCHIVEDIR}
	chmod 777 ${ARCHIVEDIR}/${FileName}.gz
   RC=$?

   if [[ $RC = 0 ]]
   then
      echo "Source File ${FileName} is compressed and archived Successfully.">>$LogFile
   else
      echo "ERROR: Error in archiving the Source File ${FileName}.gz.">>$LogFile
      exit 99
   fi

fi
RC=$?

###################################
### Remove the LogFile and Exit ###
###################################

if [[ $RC -eq 0 ]]
then
   if [ -f $LogFile ]; then rm -f $LogFile; fi
fi

exit 0
