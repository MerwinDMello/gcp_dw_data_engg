export JOBNAME='REF_RAD_ONC_PLAN_PURPOSE'

export AC_EXP_SQL_STATEMENT="SELECT 'REF_RAD_ONC_PLAN_PURPOSE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM
(
Select distinct
Case When Trim(DHD.PlanIntent)='' Then Null ELSE Trim(DHD.PlanIntent) END as Plan_Purpose_Name,
'R' as Source_System_Code,
CURRENT_TIMESTAMP(0) as DW_Last_Update_Date_Time
from EDWCR_STAGING.stg_DimPlan DHD
)src"

export AC_ACT_SQL_STATEMENT="select 'REF_RAD_ONC_PLAN_PURPOSE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.REF_RAD_ONC_PLAN_PURPOSE
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) 
as Job_Start_Date_Time FROM ${NCR_AC_SCHEMA}.ETL_JOB_RUN where Job_Name = '$JOBNAME')
"