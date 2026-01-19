
SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT trim(Status_Desc) AS Status_Desc,
 trim(Status_Type_Desc) AS Status_Type_Desc,
 Source_System_Code AS Source_System_Code
 FROM EDWCR_Staging.Ref_Status_Stg
 WHERE (trim(Status_Desc),
 trim(Status_Type_Desc)) NOT IN (sel trim(Status_Desc),
 trim(Status_Type_Desc)
 FROM EDWCR_Base_Views.REF_STATUS)
 AND DW_Last_Update_Date_Time <
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM EDWCR_DMX_AC.ETL_JOB_RUN
 WHERE Job_Name = 'J_REF_STATUS') ) A;