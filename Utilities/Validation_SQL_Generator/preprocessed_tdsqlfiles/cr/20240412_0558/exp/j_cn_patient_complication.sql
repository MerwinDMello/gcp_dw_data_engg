SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM
 (SELECT STG.CN_Patient_Complication_SID,
 STG.Nav_Patient_Id,
 STG.Core_Record_Type_Id,
 STG.Tumor_Type_Id,
 STG.Diagnosis_Result_Id,
 STG.Nav_Diagnosis_Id,
 STG.Navigator_Id,
 STG.Coid,
 'H' AS Company_Code,
 STG.Complication_Date,
 RTT.Therapy_Type_Id,
 STG.Treatment_Stopped_Ind,
 RS.Nav_Result_ID AS Outcome_Result_Id,
 STG.Complication_Text,
 STG.Specific_Complication_Text,
 STG.Comment_Text,
 STG.Hashbite_SSK,
 'N' AS Source_System_Code,
 Current_Timestamp(0) AS DW_Last_Update_Date_Time
 FROM edwcr_staging.CN_Patient_Complication_STG STG
 LEFT OUTER JOIN edwcr_base_views.Ref_Therapy_Type RTT ON COALESCE(TRIM(STG.AssociateTherapyType), 'X')=COALESCE(TRIM(RTT.Therapy_Type_Desc), 'X')
 LEFT OUTER JOIN edwcr.Ref_Result RS ON COALESCE(TRIM(STG.ComplicationOutcome), 'XX')=COALESCE(TRIM(RS.Nav_Result_Desc), 'XX')
 WHERE STG.Hashbite_SSK NOT IN
 (SELECT Hashbite_SSK
 FROM edwcr_base_views.CN_Patient_Complication) ) SRC