SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT CN_Patient_Timeline_Id,
 Nav_Patient_Id,
 Tumor_Type_Id,
 Navigator_Id,
 Coid,
 Company_Code,
 Nav_Referred_Date,
 First_Treatment_Date,
 First_Consult_Date,
 First_Imaging_Date,
 First_Medical_Oncology_Date,
 First_Radiation_Oncology_Date,
 First_Diagnosis_Date,
 First_Biopsy_Date,
 First_Surgery_Consult_Date,
 First_Surgery_Date,
 Surv_Care_Plan_Close_Date,
 Surv_Care_Plan_Resolve_Date,
 End_Treatment_Date,
 Death_Date,
 Diag_First_Trt_Day_Num,
 Diag_First_Trt_Available_Ind,
 Hashbite_SSK,
 Source_System_Code,
 CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
 FROM edwcr_staging.CN_PATIENT_TIMELINE_STG
 WHERE Hashbite_SSK NOT IN
 (SELECT Hashbite_SSK
 FROM edwcr.CN_PATIENT_TIMELINE) )A;