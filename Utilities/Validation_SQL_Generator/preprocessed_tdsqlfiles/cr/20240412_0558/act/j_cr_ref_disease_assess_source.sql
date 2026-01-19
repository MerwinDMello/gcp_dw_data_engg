SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM edwcr_base_views.REF_DISEASE_ASSESS_SOURCE
WHERE DW_Last_Update_Date_Time >=
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM edwcr_dmx_ac_base_views.ETL_JOB_RUN
 WHERE Job_Name = 'J_CR_REF_DISEASE_ASSESS_SOURCE')