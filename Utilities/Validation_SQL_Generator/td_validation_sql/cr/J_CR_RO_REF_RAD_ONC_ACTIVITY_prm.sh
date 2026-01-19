export JOBNAME='J_CR_RO_REF_RAD_ONC_ACTIVITY'

export AC_EXP_SQL_STATEMENT="SELECT 'J_CR_RO_REF_RAD_ONC_ACTIVITY'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR_STAGING.STG_DimActivity DHD"

export AC_ACT_SQL_STATEMENT="select 'J_CR_RO_REF_RAD_ONC_ACTIVITY'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.REF_RAD_ONC_ACTIVITY
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) 
as Job_Start_Date_Time FROM ${NCR_AC_SCHEMA}.ETL_JOB_RUN where Job_Name = '$JOBNAME')
"