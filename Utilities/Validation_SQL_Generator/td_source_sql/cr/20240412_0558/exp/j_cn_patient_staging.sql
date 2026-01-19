
SELECT  'J_CN_PATIENT_STAGING'||','|| cast(count(*) as varchar(20))||', ' as SOURCE_STRING 
FROM
(
SELECT 
CN_Patient_Staging_SID                  
,Hashbite_SSK                  
FROM edwcr_staging.CN_Patient_Staging_stg
where (Hashbite_SSK,Cancer_Stage_Class_Method_Code)  in ( Select Hashbite_SSK,Cancer_Stage_Class_Method_Code from
edwcr_base_views.CN_Patient_Staging ) 
and DW_Last_Update_Date_Time < (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC_base_views.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_STAGING')
) A;