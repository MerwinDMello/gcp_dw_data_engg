SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT Nav_Patient_Test_Result_SID,
 Test_Type_Id,
 Nav_Patient_Id,
 Tumor_Type_Id,
 Diagnosis_Result_Id,
 Nav_Diagnosis_Id,
 Navigator_Id,
 Coid,
 Company_Code,
 Test_Date,
 Test_Performed_Ind,
 Test_Value_Num,
 Hashbite_SSK,
 Source_System_Code,
 DW_Last_Update_Date_Time
 FROM edwcr_staging.CN_Patient_TEST_RESULT_STG STG
 WHERE STG.Hashbite_SSK NOT IN
 (SELECT Hashbite_SSK
 FROM edwcr.CN_Patient_TEST_RESULT) )A;