export JOBNAME='J_CN_PATIENT_STAGING'


export AC_EXP_SQL_STATEMENT="
SELECT  'J_CN_PATIENT_STAGING'||','|| cast(count(*) as varchar(20))||', ' as SOURCE_STRING 
FROM
(
SELECT 
CN_Patient_Staging_SID                  
,Hashbite_SSK                  
FROM $NCR_STG_SCHEMA.CN_Patient_Staging_stg
where (Hashbite_SSK,Cancer_Stage_Class_Method_Code)  in ( Select Hashbite_SSK,Cancer_Stage_Class_Method_Code from
$EDWCR_BASE_VIEWS.CN_Patient_Staging ) 
and DW_Last_Update_Date_Time < (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC_base_views.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_STAGING')

) A;"



export AC_ACT_SQL_STATEMENT="select 'J_CN_PATIENT_STAGING'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $EDWCR_BASE_VIEWS.CN_Patient_Staging WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC_BASE_VIEWS.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_STAGING');"


