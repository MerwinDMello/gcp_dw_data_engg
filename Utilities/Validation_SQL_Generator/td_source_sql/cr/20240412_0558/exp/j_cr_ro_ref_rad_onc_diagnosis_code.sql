SELECT 'J_CR_RO_Ref_Rad_Onc_Diagnosis_Code'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM 
(SELECT
ROW_NUMBER() OVER (
ORDER BY DimSiteID,
		DimDiagnosisCodeID DESC) AS Diagnosis_Code_SK,
rr.Site_SK AS Site_SK,
dp.DimDiagnosisCodeID AS Source_Diagnosis_Code_Id,
DECODE(dp.DiagnosisCode,
		'', NULL, dp.DiagnosisCode) AS Diagnosis_Code,
sc.DiagnosisSites AS Diagnosis_Site_Text,
dp.DiagnosisCodeClsSchemeId AS Diagnosis_Code_Class_Schema_Id,
DECODE(dp.DiagnosisClinicalDescriptionEN,
		'', NULL, dp.DiagnosisClinicalDescriptionEN) AS Diagnosis_Clinical_Desc,
DECODE(dp.DiagnosisFullTitleENU,
		'', NULL, dp.DiagnosisFullTitleENU) AS Diagnosis_Long_Desc,
DECODE(dp.DiagnosisTableENU,
		'', NULL, dp.DiagnosisTableENU) AS Diagnosis_Type_Code,
dp.LogID AS Log_Id,
dp.RunID AS Run_Id,
'R' AS Source_System_Code,
CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
FROM	(
						SELECT	DimDiagnosisCodeID,
						DimSiteID,
						DiagnosisCodeClsSchemeId,
							TRIM(DiagnosisCode) DiagnosisCode,
							TRIM(DiagnosisClinicalDescriptionEN) DiagnosisClinicalDescriptionEN,
				TRIM(DiagnosisFullTitleENU) DiagnosisFullTitleENU,
				TRIM(DiagnosisTableENU) DiagnosisTableENU,
							LogId,RunId 
						FROM	EDWCR_Staging.DimDiagnosisCode_STG) dp
INNER JOIN (
							SELECT	Source_Site_Id,
									Site_SK 
							FROM	EDWCR.Ref_Rad_Onc_Site) rr
	ON rr.Source_Site_Id = dp.DimSiteID
LEFT OUTER JOIN (
						SELECT	DISTINCT TRIM(DiagnosisCode) DiagnosisCode,
						DiagnosisSites 
						FROM	EDWCR_Staging.CR_SC_DIAGNOSISSITES_STG) sc
	ON sc.DiagnosisCode=dp.DiagnosisCode)STG