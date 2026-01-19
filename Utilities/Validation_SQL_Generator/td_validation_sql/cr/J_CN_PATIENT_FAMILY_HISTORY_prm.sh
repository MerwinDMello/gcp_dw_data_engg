export JOBNAME='J_CN_PATIENT_FAMILY_HISTORY'
export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_FAMILY_HISTORY'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from 
(
Select 
PatientHistoryFactID as CN_Patient_Family_History_SID,
Family_History_Query_Id,
PatientDimID as  Nav_Patient_Id,
Coid,
'H' as Company_Code,
Family_History_Value_Text,
HBSource as Hashbite_SSK,
'N' as Source_System_Code,
Current_Timestamp(0) as DW_Last_Update_Date_Time
from
$NCR_STG_SCHEMA.CN_Patient_Family_History_STG
where HBSource not in ( Select Hashbite_SSK from Edwcr_Base_Views.CN_PATIENT_FAMILY_HISTORY)
) SRC"


export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_FAMILY_HISTORY'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from Edwcr_Base_Views.CN_PATIENT_FAMILY_HISTORY
where DW_LAST_UPDATE_DATE_TIME >= (Select max(Job_Start_Date_time) from $NCR_AC_VIEW.ETL_JOB_RUN where Job_Name='J_CN_PATIENT_FAMILY_HISTORY');"