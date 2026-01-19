SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM edwcr_base_views.CN_PATIENT_HEME_DISEASE_ASSESS
WHERE DW_Last_Update_Date_Time >=
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM edwcr_dmx_ac_base_views.ETL_JOB_RUN
 WHERE Job_Name = 'J_CN_PATIENT_HEME_DISEASE_ASSESS')