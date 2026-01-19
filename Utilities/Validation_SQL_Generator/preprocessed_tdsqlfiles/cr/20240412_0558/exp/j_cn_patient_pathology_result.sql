
SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT CN_Patient_Pathology_Res_SID ,
 Pathology_Result_Type_Id ,
 Core_Record_Type_Id ,
 Nav_Patient_Id ,
 Tumor_Type_Id ,
 Diagnosis_Result_Id ,
 Nav_Diagnosis_Id ,
 Navigator_Id ,
 Coid ,
 Company_Code ,
 Result_Value_Text ,
 Hashbite_SSK ,
 Source_System_Code ,
 Current_timestamp(0) AS DW_Last_Update_Date_Time
 FROM edwcr_staging.CN_PATIENT_PATHOLOGY_RESULT_STG
 WHERE Hashbite_SSK NOT IN
 (SELECT Hashbite_SSK
 FROM edwcr.CN_PATIENT_PATHOLOGY_RESULT) ) A;