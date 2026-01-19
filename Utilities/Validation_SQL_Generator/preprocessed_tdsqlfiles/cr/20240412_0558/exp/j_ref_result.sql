
SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT trim(Nav_Result_Desc) AS Nav_Result_Desc,
 Source_System_Code AS Source_System_Code
 FROM EDWCR_Staging.REF_RESULT_Stg
 WHERE trim(Nav_Result_Desc) NOT IN (sel trim(Nav_Result_Desc)
 FROM EDWCR.REF_RESULT)
 AND DW_Last_Update_Date_Time <
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM EDWCR_DMX_AC.ETL_JOB_RUN
 WHERE Job_Name = 'J_REF_RESULT') ) A;