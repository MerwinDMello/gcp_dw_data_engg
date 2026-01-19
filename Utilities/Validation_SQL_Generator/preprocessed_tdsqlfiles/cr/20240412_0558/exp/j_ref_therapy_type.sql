
SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT Therapy_Type_Desc,
 Source_System_Code
 FROM EDWCR_Staging.Therapy_Type_stg
 WHERE trim(Therapy_Type_Desc) NOT IN (sel trim(Therapy_Type_Desc)
 FROM EdwCR.REF_THERAPY_TYPE)
 AND DW_Last_Update_Date_Time <
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM EDWCR_DMX_AC.ETL_JOB_RUN
 WHERE Job_Name = 'J_REF_THERAPY_TYPE') ) A;