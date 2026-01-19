SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM
 (SELECT dp.Fact_Patient_SK,
 rpp.Diagnosis_Code_SK,
 rp.Patient_Sk,
 dp.Diagnosis_Status_Id,
 dp.Cell_Category_Id,
 dp.Cell_Grade_Id,
 dp.Laterality_Id,
 dp.Stage_Id,
 dp.Stage_Status_Id,
 dp.Recurrence_Id,
 dp.Invasion_Id,
 dp.Confirmed_Diagnosis_Id,
 dp.Diagnosis_Type_Id,
 rr.Site_SK,
 dp.Source_Fact_Patient_Diagnosis_Id,
 dp.Diagnosis_Status_Date,
 dp.Diagnosis_Text,
 dp.Clinical_Text,
 dp.Pathology_Comment_Text,
 dp.Node_Num,
 dp.Positive_Node_Num,
 dp.Log_Id,
 dp.Run_Id,
 'R' AS Source_System_Code,
 CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
 FROM
 (SELECT Row_Number() OVER (
 ORDER BY Cast(DimSiteID AS Int),
 Cast(FactPatientDiagnosisID AS Int)) AS Fact_Patient_SK,
 DimLookupID_DiagnosisStatus AS Diagnosis_Status_Id,
 DimLookupID_CellCategory AS Cell_Category_Id,
 DimLookupID_CellGrade AS Cell_Grade_Id,
 DimLookupID_Laterality AS Laterality_Id,
 DimLookupID_Stage AS Stage_Id,
 DimLookupID_StageStatus AS Stage_Status_Id,
 DimLookupID_Recurrence AS Recurrence_Id,
 DimLookupID_Invasive AS Invasion_Id,
 DimLookupID_ConfirmedDx AS Confirmed_Diagnosis_Id,
 DimLookupID_DiagnosisType AS Diagnosis_Type_Id,
 FactPatientDiagnosisID AS Source_Fact_Patient_Diagnosis_Id,
 CAST(CAST(DiagnosisStatusDate AS TIMESTAMP (6)) AS DATE) AS Diagnosis_Status_Date,
 DiagnosisDescription AS Diagnosis_Text,
 ClinicalDescription AS Clinical_Text,
 PathologyComments AS Pathology_Comment_Text,
 Nodes AS Node_Num,
 NodesPositive AS Positive_Node_Num,
 Cast(LogID AS Int) AS Log_Id,
 Cast(RunID AS Int) AS Run_Id,
 DimDiagnosisCoDeID,
 DimPatientID,
 DimSiteID
 FROM edwcr_staging.stg_FactPatientDiagnosis) dp
 INNER JOIN EDWCR_BASE_VIEWS.Ref_Rad_Onc_Site rr ON dp.DimSiteID=rr.Source_Site_Id
 LEFT OUTER JOIN EDWCR_BASE_VIEWS.Ref_Rad_Onc_diagnosis_Code rpp ON rpp.Source_Diagnosis_Code_Id = dp.DimDiagnosisCoDeID
 AND rpp.Site_SK=rr.Site_SK
 LEFT OUTER JOIN EDWCR_BASE_VIEWS.Rad_Onc_Patient rp ON rp.Source_Patient_Id = dp.DimPatientID
 AND rp.Site_SK=rr.Site_SK)STG