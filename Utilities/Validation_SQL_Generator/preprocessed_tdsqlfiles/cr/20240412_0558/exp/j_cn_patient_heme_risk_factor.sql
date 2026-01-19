
SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT STG.PatientHemeDiagnosisFactID AS CN_Patient_Heme_Diagnosis_SID ,
 STG.PatientDimID AS Nav_Patient_Id ,
 STG.TumorTypeDimID AS Tumor_Type_Id ,
 STG.DiagnosisResultID AS Diagnosis_Result_Id ,
 STG.DiagnosisDimID AS Nav_Diagnosis_Id ,
 STG.COID AS Coid ,
 'H' AS Company_Code ,
 STG.NavigatorDimID AS Navigator_Id ,
 trim(STG.RiskFactor) AS Risk_Factor_Text ,
 trim(STG.OtherRiskFactor) AS Other_Risk_Factor_Text ,
 PREV.site_location_id AS Previous_Tumor_Site_Id ,
 OTH_PREV.site_location_id AS Other_Previous_Tumor_Site_Id ,
 STG.Hashbite_SSK ,
 'N'AS Source_System_Code ,
 CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
 FROM EDWCR_STAGING.PATIENT_HEME_RISK_FACTOR_STG STG
 LEFT JOIN EDWCR_BASE_VIEWS.Ref_Site_Location OTH_PREV ON STG.OtherTumorDiseaseSite = OTH_PREV.Site_Location_Desc
 LEFT JOIN EDWCR_BASE_VIEWS.Ref_Site_Location PREV ON STG.TumorDiseaseSite = PREV.Site_Location_Desc
 LEFT JOIN EDWCR.CN_PATIENT_HEME_RISK_FACTOR TGT ON STG.Hashbite_SSK = TGT.Hashbite_SSK
 WHERE TGT.Hashbite_SSK IS NULL
 AND TRIM(Risk_Factor_Text)<>Risk_Factor_Text ) A;