SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM edwcr_base_views.CN_Patient_Clinical_Trial
WHERE DW_Last_Update_Date_Time >=
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM EDWCR_DMX_AC_BASE_VIEWS.ETL_JOB_RUN
 WHERE Job_Name = 'J_CN_PATIENT_CLINICAL_TRIAL');