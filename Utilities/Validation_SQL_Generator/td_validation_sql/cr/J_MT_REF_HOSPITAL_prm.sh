export JOBNAME='J_MT_REF_HOSPITAL'
export AC_EXP_SQL_STATEMENT="SELECT 'J_MT_REF_HOSPITAL'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM  $NCR_STG_SCHEMA.REF_HOSPITAL_STG"

export AC_ACT_SQL_STATEMENT="select 'J_MT_REF_HOSPITAL'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.REF_HOSPITAL  WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_MT_REF_HOSPITAL');"
