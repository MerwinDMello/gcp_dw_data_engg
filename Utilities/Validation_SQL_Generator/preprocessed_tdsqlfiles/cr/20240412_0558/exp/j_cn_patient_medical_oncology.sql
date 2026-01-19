
SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT stg.CN_Patient_Medical_Oncology_ID ,
 stg.Treatment_Type_Id ,
 ref.Facility_Id ,
 stg.Core_Record_Type_Id ,
 stg.Med_Spcl_Physician_Id ,
 stg.Nav_Patient_Id ,
 stg.Tumor_Type_Id ,
 stg.Diagnosis_Result_Id ,
 stg.Nav_Diagnosis_Id ,
 stg.Navigator_Id ,
 stg.Coid ,
 stg.Company_Code ,
 stg.Core_Record_Date ,
 stg.Treatment_Start_Date ,
 stg.Treatment_End_Date ,
 stg.Estimated_End_Date ,
 stg.Drug_Name ,
 stg.Dose_Dense_Chemo_Ind ,
 stg.Drug_Dose_Amt_Text ,
 stg.Drug_Dose_Measurement_Text ,
 stg.Drug_Available_Ind ,
 stg.Drug_Qty ,
 stg.Cycle_Num ,
 stg.Cycle_Frequency_Text ,
 stg.Medical_Oncology_Reason_Text ,
 stg.Terminated_Ind ,
 stg.Treatment_Therapy_Schedule_Cd ,
 stg.Comment_Text ,
 stg.Hashbite_SSK ,
 stg.Source_System_Code ,
 Current_timestamp(0) AS DW_Last_Update_Date_Time
 FROM edwcr_staging.CN_PATIENT_MEDICAL_ONCOLOGY_Stg stg
 LEFT JOIN EDWCR.Ref_Facility REF ON stg.Medical_Oncology_Facility_Id = Ref.Facility_Name
 WHERE Hashbite_SSK NOT IN
 (SELECT Hashbite_SSK
 FROM edwcr_base_views.CN_PATIENT_MEDICAL_ONCOLOGY) ) A;