
SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT trim(Diagnosis_Detail_Desc) AS Diagnosis_Detail_Desc,
 trim(Diagnosis_Indicator_Text) AS Diagnosis_Indicator_Text,
 Source_System_Code AS Source_System_Code
 FROM EDWCR_Staging.Ref_Diagnosis_Detail_Stg
 WHERE (trim(Diagnosis_Detail_Desc),
 trim(Diagnosis_Indicator_Text)) NOT IN (sel trim(Diagnosis_Detail_Desc),
 trim(Diagnosis_Indicator_Text)
 FROM EDWCR.Ref_Diagnosis_Detail)
 AND DW_Last_Update_Date_Time <
 (SELECT MAX(Job_Start_Date_Time) AS Job_Start_Date_Time
 FROM EDWCR_DMX_AC.ETL_JOB_RUN
 WHERE Job_Name = 'J_REF_DIAGNOSIS_DETAIL') ) A;