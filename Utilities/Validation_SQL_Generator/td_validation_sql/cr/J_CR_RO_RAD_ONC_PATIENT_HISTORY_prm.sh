export JOBNAME='J_CR_RO_RAD_ONC_PATIENT_HISTORY'

export AC_EXP_SQL_STATEMENT="SELECT 'J_CR_RO_RAD_ONC_PATIENT_HISTORY'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR_STAGING.stg_DIMPATIENT"

export AC_ACT_SQL_STATEMENT="select 'J_CR_RO_RAD_ONC_PATIENT_HISTORY'||','|| cast(count(distinct(Patient_SK)) as varchar(20))||',' as SOURCE_STRING 
FROM $EDWCR_BASE_VIEWS.Rad_Onc_Patient_History
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM ${NCR_AC_SCHEMA}.ETL_JOB_RUN where Job_Name = '$JOBNAME')
"


