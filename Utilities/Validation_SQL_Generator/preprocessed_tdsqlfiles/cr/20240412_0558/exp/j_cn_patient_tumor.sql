SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM
 (SELECT STG.CN_Patient_Tumor_SID,
 STG.Nav_Patient_Id,
 STG.Tumor_Type_Id,
 STG.Navigator_Id,
 STG.Coid,
 'H' AS Company_Code,
 STG.Electronic_Folder_Id_Text,
 RF1.Facility_Id AS Referral_Source_Facility_Id,
 RS.Status_ID AS Nav_Status_Id,
 STG.Identification_Period_Text,
 STG.Referral_Date,
 STG.Referring_Physician_Id,
 STG.Nav_End_Reason_Text,
 STG.Treatment_End_Reason_Text,
 PD.Physician_Id AS Treatment_End_Physician_Id,
 RF2.Facility_Id AS Treatment_End_Facility_Id,
 STG.Hashbite_SSK,
 'N' AS Source_System_Code,
 Current_Timestamp(0) AS DW_Last_Update_Date_Time
 FROM edwcr_staging.CN_Patient_Tumor_STG STG
 LEFT OUTER JOIN edwcr.Ref_Facility RF1 ON STG.TumorReferralSource=RF1.Facility_Name
 LEFT OUTER JOIN edwcr.Ref_Status RS ON STG.NavigationStatus = RS.Status_Desc
 LEFT OUTER JOIN edwcr.CN_Physician_Detail PD ON STG.EndTreatmentPhysician=PD.Physician_Name
 AND PD.Physician_Phone_Num IS NULL
 LEFT OUTER JOIN edwcr.Ref_Facility RF2 ON STG.EndTreatmentLocation=RF2.Facility_Name qualify row_number() over(PARTITION BY CN_Patient_Tumor_SID
 ORDER BY PD.Physician_ID DESC)=1
 WHERE STG.Hashbite_SSK NOT IN
 (SELECT Hashbite_SSK
 FROM edwcr.CN_Patient_Tumor) ) SRC