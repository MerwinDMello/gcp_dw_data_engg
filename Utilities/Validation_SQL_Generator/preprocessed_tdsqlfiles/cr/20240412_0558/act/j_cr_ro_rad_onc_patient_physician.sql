SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM edwcr.Rad_Onc_Patient_Physician
WHERE DW_Last_Update_Date_Time >=
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM edwcr_dmx_ac.ETL_JOB_RUN
 WHERE Job_Name = 'J_CR_RO_Rad_Onc_Patient_Physician')