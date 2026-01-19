SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT CN_Patient_Surgery_SID,
 RS.Side_ID,
 RF.Facility_Id,
 Surgery_Type_Id,
 Core_Record_Type_Id,
 Med_Spcl_Physician_Id,
 Referring_Physician_Id,
 Nav_Patient_Id,
 Tumor_Type_Id,
 Diagnosis_Result_Id,
 Nav_Diagnosis_Id,
 Navigator_Id,
 Coid,
 Company_Code,
 Surgery_Date,
 General_Surgery_Type_Text,
 Reconstructive_Offered_Ind,
 Palliative_Ind,
 Comment_Text,
 Hashbite_SSK,
 STG.Source_System_Code,
 STG.DW_Last_Update_Date_Time
 FROM edwcr_staging.CN_PATIENT_SURGERY_STG STG
 LEFT JOIN edwcr.Ref_Side RS ON Trim(STG.SurgerySide) = Trim(RS.Side_Desc)
 LEFT JOIN edwcr.Ref_Facility RF ON Trim(STG.SurgeryFacility) = Trim(RF.facility_Name)
 WHERE STG.Hashbite_SSK NOT IN
 (SELECT Hashbite_SSK
 FROM edwcr.CN_PATIENT_SURGERY) )A;