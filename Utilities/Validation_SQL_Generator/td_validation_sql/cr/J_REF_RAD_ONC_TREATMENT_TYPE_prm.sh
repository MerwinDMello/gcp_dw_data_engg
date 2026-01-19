
export JOBNAME='J_REF_RAD_ONC_TREATMENT_TYPE'

export AC_EXP_SQL_STATEMENT="SELECT 'J_REF_RAD_ONC_TREATMENT_TYPE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR_STAGING.stg_SC_Modalities"

export AC_ACT_SQL_STATEMENT="select 'J_REF_RAD_ONC_TREATMENT_TYPE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $EDWCR_BASE_VIEWS.Ref_Rad_Onc_Treatment_type
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM ${NCR_AC_SCHEMA}.ETL_JOB_RUN where Job_Name = '$JOBNAME')
"


