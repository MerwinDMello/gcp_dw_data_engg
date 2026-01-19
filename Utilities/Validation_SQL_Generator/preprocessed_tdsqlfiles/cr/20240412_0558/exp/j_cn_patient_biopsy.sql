SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT CN_Patient_Biopsy_SID,
 Core_Record_Type_Id,
 Med_Spcl_Physician_Id,
 Referring_Physician_Id,
 Biopsy_Type_Id,
 Biopsy_Result_Id,
 FAC.Facility_Id,
 L.Site_Location_Id,
 S.Physician_Specialty_Id,
 Nav_Patient_Id,
 Tumor_Type_Id,
 Diagnosis_Result_Id,
 Nav_Diagnosis_Id,
 Navigator_Id,
 Coid,
 Company_Code,
 Biopsy_Date,
 Biopsy_Clip_Sw,
 Biopsy_Needle_Sw,
 General_Biopsy_Type_Text,
 Comment_Text,
 Hashbite_SSK,
 STG.Source_System_Code,
 STG.DW_Last_Update_Date_Time
 FROM edwcr_staging.CN_PATIENT_BIOPSY_STG STG
 LEFT JOIN edwcr_base_views.REF_FACILITY FAC ON STG.BiopsyFacility = FAC.Facility_Name
 LEFT JOIN edwcr_base_views.Ref_Site_Location L ON STG.BiopsySite = L.Site_Location_Desc
 LEFT JOIN edwcr_base_views.Ref_Physician_Specialty S ON STG.BiopsyPhysicianType = S.Physician_Specialty_Desc
 WHERE STG.Hashbite_SSK NOT IN
 (SELECT Hashbite_SSK
 FROM edwcr_base_views.CN_PATIENT_BIOPSY) )A;