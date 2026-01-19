SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM
 (SELECT STG.CN_Patient_Diagnosis_SID,
 STG.Nav_Patient_Id,
 STG.Tumor_Type_Id,
 STG.Diagnosis_Result_Id,
 STG.Nav_Diagnosis_Id,
 STG.Navigator_Id,
 STG.Coid,
 'H' AS Company_Code,
 STG.General_Diagnosis_Name,
 STG.Diagnosis_Date,
 RDD.Diagnosis_Detail_Id,
 RS.Side_ID AS Diagnosis_Side_Id,
 STG.Hashbite_SSK,
 'N' AS Source_System_Code,
 Current_Timestamp(0) AS DW_Last_Update_Date_Time
 FROM edwcr_staging.CN_Patient_Diagnosis_STG STG
 LEFT OUTER JOIN edwcr.Ref_Side RS ON STG.DiagnosisSide=RS.Side_Desc
 LEFT OUTER JOIN edwcr.Ref_Diagnosis_Detail RDD ON COALESCE(TRIM(STG.DiagnosisMetastatic), 'X')=COALESCE(TRIM(RDD.Diagnosis_Detail_Desc), 'X')
 AND COALESCE(TRIM(STG.DiagnosisIndicator), 'XX')=COALESCE(TRIM(RDD.Diagnosis_Indicator_Text), 'XX')
 WHERE STG.Hashbite_SSK NOT IN
 (SELECT Hashbite_SSK
 FROM edwcr.CN_Patient_Diagnosis)) SRC