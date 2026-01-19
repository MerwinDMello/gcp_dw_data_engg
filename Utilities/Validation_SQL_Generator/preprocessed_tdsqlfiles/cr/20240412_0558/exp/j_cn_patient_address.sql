SELECT CONCAT(count(*), ', ') AS SOURCE_STRING
FROM
 (SELECT *
 FROM edwcr_staging.CN_PATIENT_ADDRESS_Stg
 WHERE Nav_Patient_Id NOT IN
 (SELECT Nav_Patient_Id
 FROM edwcr.CN_Patient_Address)
 AND DW_Last_Update_Date_Time <
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM EDWCR_DMX_AC.ETL_JOB_RUN
 WHERE Job_Name = 'J_CN_PATIENT_ADDRESS') )A;