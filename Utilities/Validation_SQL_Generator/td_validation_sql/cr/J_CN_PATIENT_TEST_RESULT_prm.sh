export JOBNAME='J_CN_PATIENT_TEST_RESULT'
export AC_EXP_SQL_STATEMENT="SELECT 'J_CN_PATIENT_TEST_RESULT'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
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
From $NCR_STG_SCHEMA.CN_Patient_TEST_RESULT_STG STG 
where STG.Hashbite_SSK not in ( Select Hashbite_SSK from $NCR_TGT_SCHEMA.CN_Patient_TEST_RESULT)
)A;"
export AC_ACT_SQL_STATEMENT="select 'J_CN_PATIENT_TEST_RESULT'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.CN_Patient_TEST_RESULT  WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_TEST_RESULT');"
