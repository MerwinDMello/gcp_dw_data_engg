set -x

export View_Name=vwDynamicRoleAttachment
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
      ,[Created_User_Unit_Id]
      ,[Created_User_UnitName]
      ,[Created_User_UnitCode]
      ,[Created_User_Hospital]
      ,[Created_User_FacilityCode]
      ,[User_Id]
      ,[User_Name]
      ,[User_Full_Name]
      ,[User_Actual_Role]
      ,[User_Recent_Unit_Id]
      ,[User_Recent_UnitName]
      ,[User_Recent_UnitCode]
      ,[User_Recent_Hospital]
      ,[User_Recent_FacilityCode]
      ,[DynamicRole_DateTime]
      ,[DynamicRole_Name]
      ,[DynamicRole_Label]
      ,[DynamicRole_Phone]
      ,[InferredUpdate]
      ,[LastTimeStamp]
     ,'$DBname' As Databasename
FROM $DBname.dbo.vwDynamicRoleAttachment
where 
(LastTimeStamp > '${From_Date}' AND LastTimeStamp <= '${To_Date}')  OR (InferredUpdate=1)
)a"


##############ACTUAL##################


export TARGET_DIR=/etl/ST/EDWCI/SrcFiles
export AC_ACT_INPUT_FILE='J_MHB_Vw_Dynamic_Role_Attachment_stg_'${DBname}'.txt' 
export P_ACT_Delimiter='"|"' 
export P_ACT_Control_Total_Field='1,'
export P_ACT_Number_of_Fields='26'
export P_ACT_Control_Total_Type='1,'
