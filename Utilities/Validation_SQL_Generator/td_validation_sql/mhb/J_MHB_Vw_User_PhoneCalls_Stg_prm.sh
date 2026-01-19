 set -x

export View_Name=vwUserPhoneCalls
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
call_id ,
unit_id,
unitname,
unitcode,
Hospital, 
FacilityCode,
Call_DateTime, 
Direction, 
Call_Status, 
Call_Length,
user_id,
User_Name,
User_Full_Name, 
User_Role, 
User_PhoneNumber, 
Other_Party_Name,
Other_Party_Number,
Responder_5_Call,
MeanOpinionScore,
'$DBname'  As Databasename
FROM $DBname.dbo.vwUserPhoneCalls
where 
(LastTimeStamp > '${From_Date}'
AND LastTimeStamp <= '${To_Date}') OR (InferredUpdate=1)
)a"


##############ACTUAL##################


export TARGET_DIR=/etl/ST/EDWCI/SrcFiles
export AC_ACT_INPUT_FILE='J_MHB_Vw_User_PhoneCalls_Stg_'${DBname}'.txt' 
export P_ACT_Delimiter='"|"' 
export P_ACT_Control_Total_Field='1,'
export P_ACT_Number_of_Fields='20'
export P_ACT_Control_Total_Type='1,'

