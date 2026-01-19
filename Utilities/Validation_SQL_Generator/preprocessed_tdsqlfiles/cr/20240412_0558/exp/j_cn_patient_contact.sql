
SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT CN_Patient_Contact_SID ,
 Hashbite_SSK
 FROM edwcr_staging.CN_Patient_Contact_stg
 WHERE Hashbite_SSK NOT IN
 (SELECT Hashbite_SSK
 FROM edwcr.CN_PATIENT_CONTACT)
 AND DW_Last_Update_Date_Time <
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM EDWCR_DMX_AC.ETL_JOB_RUN
 WHERE Job_Name = 'J_CN_PATIENT_CONTACT') ) A;