SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT CN_Patient_Core_Adherence_SID,
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
 FROM edwcr_staging.CN_PATIENT_CORE_ADHERENCE_STG STG
 WHERE STG.Hashbite_SSK NOT IN
 (SELECT Hashbite_SSK
 FROM edwcr.CN_PATIENT_CORE_ADHERENCE) )A;