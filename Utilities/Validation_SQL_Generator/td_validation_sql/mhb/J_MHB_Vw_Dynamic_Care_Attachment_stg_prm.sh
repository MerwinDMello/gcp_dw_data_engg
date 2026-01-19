set -x

export View_Name=vwDynamicCareAttachment
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
      [Trail_Id]
      ,[Created_User_Id]
      ,[Created_User_Name]
      ,[Created_User_Full_Name]
      ,[Created_User_Role]
      ,[Created_Unit_Id]
      ,[Created_UnitName]
      ,[Created_UnitCode]
      ,[Created_Hospital]
      ,[Created_FacilityCode]
      ,[Patient_Id]
      ,[Patient_Name]
      ,[Patient_MRN]
      ,[Patient_VisitNumber]
      ,[Patient_FacilityCode]
      ,[Owner_User_Id]
      ,[Owner_User_Name]
      ,[Owner_User_Full_Name]
      ,[Owner_User_Role]
      ,[Owner_Unit_Id]
      ,[Owner_UnitName]
      ,[Owner_UnitCode]
      ,[Owner_Hospital]
      ,[Owner_FacilityCode]
      ,[Attach_DateTime]
      ,[Attach_Source]
      ,[InferredUpdate]
      ,[LastTimeStamp]
 ,'$DBname' As Databasename
FROM $DBname.dbo.vwDynamicCareAttachment
where 
(LastTimeStamp > '${From_Date}'
AND LastTimeStamp <= '${To_Date}') OR (InferredUpdate=1)
)a"


##############ACTUAL##################


export TARGET_DIR=/etl/ST/EDWCI/SrcFiles
export AC_ACT_INPUT_FILE='J_MHB_Vw_Dynamic_Care_Attachment_stg_'${DBname}'.txt' 
export P_ACT_Delimiter='|' 
export P_ACT_Control_Total_Field='1,'
export P_ACT_Number_of_Fields='29'
export P_ACT_Control_Total_Type='1,'