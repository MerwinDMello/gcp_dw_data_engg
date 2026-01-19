SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM EDWCR.cn_patient_heme_risk_factor
WHERE DW_Last_Update_Date_Time >=
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM EDWCR_DMX_AC.ETL_JOB_RUN
 WHERE Job_Name = 'J_CN_PATIENT_HEME_RISK_FACTOR');