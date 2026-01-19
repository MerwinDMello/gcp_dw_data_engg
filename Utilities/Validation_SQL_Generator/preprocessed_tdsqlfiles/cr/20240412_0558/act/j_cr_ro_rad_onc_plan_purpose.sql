SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM edwcr.REF_RAD_ONC_PLAN_PURPOSE
WHERE DW_Last_Update_Date_Time >=
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM edwcr_dmx_ac.ETL_JOB_RUN
 WHERE Job_Name = 'REF_RAD_ONC_PLAN_PURPOSE')