
SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT Breast_Cancer_Type_Desc,
 Source_System_Code
 FROM EdwCR_Staging.Ref_Breast_Cancer_Type_Stg
 WHERE trim(Breast_Cancer_Type_Desc) NOT IN (sel trim(Breast_Cancer_Type_Desc)
 FROM EdwCR.Ref_Breast_Cancer_Type)
 AND DW_Last_Update_Date_Time <
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM EDWCR_DMX_AC.ETL_JOB_RUN
 WHERE Job_Name = 'J_REF_BREAST_CANCER_TYPE') ) A;