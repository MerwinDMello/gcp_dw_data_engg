SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM edwcr.CN_Patient_Mltp_Disciplinary_Meeting
WHERE DW_Last_Update_Date_Time >=
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM EDWCR_DMX_AC.ETL_JOB_RUN
 WHERE Job_Name = 'J_CN_PATIENT_MLTP_DISP_MEETING');