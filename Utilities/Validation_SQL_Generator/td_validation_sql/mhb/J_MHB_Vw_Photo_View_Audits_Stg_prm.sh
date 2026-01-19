set -x

export View_Name=vwPhotoViewAudits 
export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
JOBNAME1=${Job_Name}_${DBname}
export JOBNAME=`echo $JOBNAME1|cut -c1-100`
export SQL_IMOBILE_DB='SQL_IMOBILE'
#export SQL_IMOBILE_USER='ImobileDataLoad_User'
#export SQL_IMOBILE_PASSWORD='U$sBe0XI95'

###########EXPECTED###################

export AC_EXP_SQL_STATEMENT="SELECT '${Job_Name}'+'_'+'${DBname}'+ ',' + cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING
FROM (
SELECT	
Trail_Id, 
Unit_Id, 
UnitName,
UnitCode,
Hospital, 
FacilityCode,
User_Id, 
User_Name, 
User_Full_Name, 
User_Role, 
Patient_Id, 
Patient_Name,
Patient_MRN, 
Patient_VisitNumber, 
Patient_FacilityCode, 
Photo_FileName,
Photo_Viewed_DateTime,
'$DBname' As Databasename
FROM $DBname.dbo.vwPhotoViewAudits
where 
(LastTimeStamp > '${From_Date}'
AND LastTimeStamp <= '${To_Date}') OR (InferredUpdate=1)
)a"


##############ACTUAL##################


export TARGET_DIR=/etl/ST/EDWCI/SrcFiles
export AC_ACT_INPUT_FILE='J_MHB_Vw_Photo_View_Audits_Stg_'${DBname}'.txt' 
export P_ACT_Delimiter='|' 
export P_ACT_Control_Total_Field='1,'
export P_ACT_Number_of_Fields='18'
export P_ACT_Control_Total_Type='1,'
