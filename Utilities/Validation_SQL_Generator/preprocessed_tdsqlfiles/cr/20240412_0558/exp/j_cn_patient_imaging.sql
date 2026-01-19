SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM
 (SELECT STG.CN_Patient_Imaging_SID,
 STG.Core_Record_Type_Id,
 STG.Nav_Patient_Id,
 STG.Med_Spcl_Physician_Id,
 STG.Tumor_Type_Id,
 STG.Diagnosis_Result_Id,
 STG.Nav_Diagnosis_Id,
 STG.Navigator_Id,
 STG.Coid,
 'H' AS Company_Code,
 STG.Imaging_Type_Id,
 STG.Imaging_Date,
 RIM.Imaging_Mode_Id AS Imaging_Mode_Id,
 RS.Side_Id AS Imaging_Area_Side_Id,
 STG.Imaging_Location_Text,
 RF.Facility_Id AS Imaging_Facility_Id,
 CASE
 WHEN (STG.Birad_Scale_Code)='Results not available' THEN NULL
 ELSE STG.Birad_Scale_Code
 END AS Birad_Scale_Code, --STG.Birad_Scale_Code,
 STG.Comment_Text,
 DS.Status_Id AS Disease_Status_Id,
 TS.Status_Id AS Treatment_Status_Id,
 STG.Other_Image_Type_Text,
 CASE
 WHEN Initial_Diagnosis_Ind = 'Yes' THEN 'Y'
 WHEN Initial_Diagnosis_Ind = 'No' THEN 'N'
 ELSE 'U'
 END AS Initial_Diagnosis_Ind,
 CASE
 WHEN Disease_Monitoring_Ind = 'Yes' THEN 'Y'
 WHEN Disease_Monitoring_Ind = 'No' THEN 'N'
 ELSE 'U'
 END AS Disease_Monitoring_Ind,
 STG.Radiology_Result_Text,
 STG.Hashbite_SSK,
 'N' Source_System_Code,
 Current_timestamp(0) AS DW_Last_Update_Date_Time
 FROM EDWCR_STAGING.CN_Patient_Imaging_STG STG
 LEFT OUTER JOIN EDWCR_BASE_VIEWS.Ref_Imaging_Mode RIM ON COALESCE(TRIM(STG.ImageMode), 'X')=COALESCE(TRIM(RIM.Imaging_Mode_Desc), 'X')
 LEFT OUTER JOIN EDWCR_BASE_VIEWS.Ref_Side RS ON COALESCE(TRIM(STG.ImageArea), 'XX')=COALESCE(TRIM(RS.Side_Desc), 'XX')
 LEFT OUTER JOIN EDWCR_BASE_VIEWS.Ref_Facility RF ON COALESCE(TRIM(STG.ImageCenter), 'XXX')=COALESCE(TRIM(RF.Facility_Name), 'XXX')
 LEFT OUTER JOIN EDWCR_BASE_VIEWS.Ref_Status DS ON COALESCE(TRIM(STG.Disease_Status), 'XXX')=COALESCE(TRIM(DS.Status_Desc), 'XXX')
 AND DS.Status_Type_Desc='Disease'
 LEFT OUTER JOIN EDWCR_BASE_VIEWS.Ref_Status TS ON COALESCE(TRIM(STG.Treatment_Status), 'XXX')=COALESCE(TRIM(TS.Status_Desc), 'XXX')
 AND TS.Status_Type_Desc='Treatment'
 WHERE Hashbite_SSK NOT IN
 (SELECT Hashbite_SSK
 FROM EDWCR_BASE_VIEWS.CN_Patient_Imaging) ) SRC