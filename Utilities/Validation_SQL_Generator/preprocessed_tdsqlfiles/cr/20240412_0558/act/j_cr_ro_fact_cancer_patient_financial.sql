SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM edwcr.Fact_Cancer_Patient_Financial
WHERE DW_Last_Update_Date_Time >=
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM edwcr_dmx_ac.ETL_JOB_RUN
 WHERE Job_Name = 'J_CR_RO_Fact_Cancer_Patient_Financial')