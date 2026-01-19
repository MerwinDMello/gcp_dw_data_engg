SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT CN_Patient_Surgery_Recstr_SID,
 Core_Record_Type_Id,
 RS.Side_ID,
 Med_Spcl_Physician_Id,
 Nav_Patient_Id,
 Tumor_Type_Id,
 Diagnosis_Result_Id,
 Nav_Diagnosis_Id,
 Navigator_Id,
 Coid,
 Company_Code,
 Recstr_Date,
 Surgery_Recstr_Type_Text,
 Declined_Ind,
 Hashbite_SSK,
 STG.Source_System_Code,
 STG.DW_Last_Update_Date_Time
 FROM edwcr_staging.CN_Patient_Surgery_Reconstruction_STG STG
 LEFT JOIN edwcr.Ref_Side RS ON Trim(STG.ReconSurgerySide) = Trim(RS.Side_Desc)
 WHERE STG.Hashbite_SSK NOT IN
 (SELECT Hashbite_SSK
 FROM edwcr.CN_Patient_Surgery_Reconstruction) )A;