#Script Name	: TMS_File_Copy.sh
#Description	: This Script copies files from Temp folder Temp_Zip folder )
#Created By 	: Syntel
#Created On	: 02.10.2020
#Syntax	        : TMS_File_Copy.sh {file frequency mode. EX: TMS_File_Copy.sh DAILY}



#Change History:


#Version No.		Modified By		Modified Date   Comments
############		############		##############	 #########

#1.0			Syntel		        02.10.2020       Created


#########################################################
### Variable Declaration and Parameter Initialization ###
#########################################################
set -x

archiveDate=`date +%F-%T`
LogFile=/etl/ST/EDWHR/LogFiles/TMS_${1}_FileCopy.log
TEMPDIR=/etl/ST/EDWHR/SrcFiles/TMS_${1}_Temp
TEMPDIR_ZIP=/etl/ST/EDWHR/SrcFiles/TMS_${1}_Temp_Zip
SRCDIR=/etl/ST/EDWHR/SrcFiles
rc=0
 
cd ${TEMPDIR}

 rc=$?

 if [[ $rc -eq 0 ]]
       then
          echo "Directory changed to Temp directory /etl/ST/EDWHR/SrcFiles/TMS_${1}_Temp Successfully.">$LogFile
       else
 
          echo "ERROR in changing directory to  Temp directory /etl/ST/EDWHR/SrcFiles/TMS_${1}_Temp.">>$LogFile  
          exit 99
 fi


cp /etl/ST/EDWHR/SrcFiles/TMS_${1}_Temp/* /etl/ST/EDWHR/SrcFiles/TMS_${1}_Temp_Zip

  rc=$?

 if [[ $rc -eq 0 ]]
       then
          echo "Files copied to Temp directory /etl/ST/EDWHR/SrcFiles/TMS_${1}_Temp_Zip Successfully.">>$LogFile
       else
 
          echo "ERROR in copying the files to  Temp directory /etl/ST/EDWHR/SrcFiles/TMS_${1}_Temp_Zip.">>$LogFile  
          exit 99
 fi
