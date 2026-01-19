SELECT 'J_CN_PATIENT_TEST_RESULT'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
Select
      Nav_Patient_Test_Result_SID ,
      Test_Type_Id ,
      Nav_Patient_Id ,
      Tumor_Type_Id ,
      Diagnosis_Result_Id ,
      Nav_Diagnosis_Id ,
      Navigator_Id ,
      Coid ,
      Company_Code ,
      Test_Date ,
      Test_Performed_Ind ,
      Test_Value_Num ,
      Hashbite_SSK ,
      Source_System_Code ,
      DW_Last_Update_Date_Time 
From edwcr_staging.CN_Patient_TEST_RESULT_STG STG 
where STG.Hashbite_SSK not in ( Select Hashbite_SSK from edwcr.CN_Patient_TEST_RESULT)
)A;