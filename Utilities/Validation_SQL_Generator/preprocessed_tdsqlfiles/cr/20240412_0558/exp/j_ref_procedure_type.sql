SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT Procedure_Type_Desc,
 Procedure_Sub_Type_Desc,
 Source_System_Code
 FROM EDWCR_Staging.Procedure_Type_stg
 WHERE (trim(Procedure_Type_Desc),
 trim(COALESCE(Procedure_Sub_Type_Desc, ''))) NOT IN (sel trim(Procedure_Type_Desc),
 trim(COALESCE(Procedure_Sub_Type_Desc, ''))
 FROM EDWCR_BASE_VIEWS.REF_PROCEDURE_TYPE
 WHERE Procedure_Type_Desc IS NOT NULL )
 AND DW_Last_Update_Date_Time <
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM EDWCR_DMX_AC.ETL_JOB_RUN
 WHERE Job_Name = 'J_REF_PROCEDURE_TYPE') ) A;