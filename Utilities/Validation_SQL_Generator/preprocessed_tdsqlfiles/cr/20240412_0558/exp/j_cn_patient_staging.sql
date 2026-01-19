
SELECT CONCAT(count(*), ', ') AS SOURCE_STRING
FROM
 (SELECT CN_Patient_Staging_SID ,
 Hashbite_SSK
 FROM edwcr_staging.CN_Patient_Staging_stg
 WHERE (Hashbite_SSK,
 Cancer_Stage_Class_Method_Code) IN
 (SELECT Hashbite_SSK,
 Cancer_Stage_Class_Method_Code
 FROM edwcr_base_views.CN_Patient_Staging)
 AND DW_Last_Update_Date_Time <
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM EDWCR_DMX_AC_base_views.ETL_JOB_RUN
 WHERE Job_Name = 'J_CN_PATIENT_STAGING') ) A;