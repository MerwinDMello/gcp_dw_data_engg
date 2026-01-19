export JOBNAME='J_CN_PATIENT_CONSULTATION'



export AC_EXP_SQL_STATEMENT="
SELECT	'J_CN_PATIENT_CONSULTATION'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT 
CN_Patient_Consultation_SID                 
,Hashbite_SSK                  
FROM $NCR_STG_SCHEMA.CN_Patient_Consultation_stg
where Hashbite_SSK not in ( Select Hashbite_SSK from $EDWCR_BASE_VIEWS.CN_PATIENT_CONSULTATION )
and DW_Last_Update_Date_Time < (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_CONSULTATION')

) A;"


export AC_ACT_SQL_STATEMENT="select 'J_CN_PATIENT_CONSULTATION'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $EDWCR_BASE_VIEWS.CN_PATIENT_CONSULTATION WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_CONSULTATION');"

