export JOBNAME='J_CR_RO_RAD_ONC_ACTVT_PRIORITY'

export AC_EXP_SQL_STATEMENT="SELECT 'J_CR_RO_RAD_ONC_ACTVT_PRIORITY'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM
(Select distinct
Case When Trim(DHD.ActivityPriority)='' Then Null ELSE Trim(DHD.ActivityPriority) END as Activity_Priority_Desc,
'R' as Source_System_Code,
CURRENT_TIMESTAMP(0) as DW_Last_Update_Date_Time
from EDWCR_STAGING.stg_DimActivityTransaction DHD
)src
where src.Activity_Priority_Desc is not null"

export AC_ACT_SQL_STATEMENT="select 'J_CR_RO_RAD_ONC_ACTVT_PRIORITY'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.REF_RAD_ONC_ACTIVITY_PRIORITY
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) 
as Job_Start_Date_Time FROM ${NCR_AC_SCHEMA}.ETL_JOB_RUN where Job_Name = '$JOBNAME')
"