SELECT 'J_CN_PATIENT_CORE_ADHERENCE'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
Select
      CN_Patient_Core_Adherence_SID,
      Core_Adherence_Measure_Id,
      Nav_Patient_Id,
      Tumor_Type_Id,
      Diagnosis_Result_Id,
      Nav_Diagnosis_Id,
      Navigator_Id,
      Coid,
      Company_Code,
      Core_Adherence_Measure_Text,
      Hashbite_SSK,
      Source_System_Code,
      DW_Last_Update_Date_Time
From edwcr_staging.CN_PATIENT_CORE_ADHERENCE_STG STG 
where STG.Hashbite_SSK not in ( Select Hashbite_SSK from edwcr.CN_PATIENT_CORE_ADHERENCE)
)A;