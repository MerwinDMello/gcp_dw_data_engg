SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM edwcr.CN_Patient_Genetics_Testing
WHERE DW_LAST_UPDATE_DATE_TIME >=
 (SELECT max(Job_Start_Date_time)
 FROM edwcr_dmx_ac_base_views.ETL_JOB_RUN
 WHERE Job_Name='J_CN_PATIENT_GENETICS_TESTING');