SELECT 'J_CR_RO_Fact_Rad_Onc_Course_Diagnosis'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM 
(SELECT
ROW_NUMBER() OVER (
ORDER BY DimSiteID,
		FactCourseDiagnosisID DESC) AS Fact_Course_Diagnosis_SK,
ra.Fact_Patient_Diagnosis_SK AS Fact_Patient_Diagnosis_SK,
ra1.Patient_Course_SK AS Patient_Course_SK,
dc.Diagnosis_Code_SK AS Diagnosis_Code_SK,
rr.Site_SK AS Site_SK,
dp.FactCourseDiagnosisID AS Source_Fact_Course_Diagnosis_Id,
DECODE(dp.IsPrimary, 1, 'Y', 0, 'N')  AS Primary_Course_Ind,
dp.LogID AS Log_Id,
dp.RunID AS Run_Id,
'R' AS Source_System_Code,
CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
FROM	(
		SELECT	FactCourseDiagnosisID,
			FactPatientDiagnosisID,DimCourseID,
				DimDiagnosisCodeID,DimSiteID,
			IsPrimary,LogId,RunId 
		FROM	EDWCR_Staging.stg_FACTCOURSEDIAGNOSIS) dp
INNER JOIN (
			SELECT	Source_Site_Id,
					Site_SK 
			FROM	EDWCR.Ref_Rad_Onc_Site) rr
	ON rr.Source_Site_Id = dp.DimSiteID
LEFT OUTER JOIN 
(
SELECT	Source_Fact_Patient_Diagnosis_Id,
			Fact_Patient_Diagnosis_SK,Source_Site_Id 
FROM
(
		SELECT	Source_Fact_Patient_Diagnosis_Id,
			Fact_Patient_Diagnosis_SK,
				Site_SK 
		FROM	EDWCR.Fact_rad_Onc_Patient_Diagnosis) ra
		
		INNER JOIN (
			SELECT	Source_Site_Id,
					Site_SK 
			FROM	EDWCR.Ref_Rad_Onc_Site) rs
	ON rs.Site_Sk=ra.Site_SK)ra
	ON dp.FactPatientDiagnosisID = ra.Source_Fact_Patient_Diagnosis_Id
	AND ra.Source_Site_Id=dp.DimSiteID
 
LEFT OUTER JOIN 
(
SELECT	Source_Patient_Course_Id,
			Patient_Course_SK,Source_Site_Id 
FROM	
(
		SELECT	Source_Patient_Course_Id,
			Patient_Course_SK,Site_SK 
		FROM	EDWCR.Rad_Onc_Patient_Course) ra1
		
		INNER JOIN (
			SELECT	Source_Site_Id,
					Site_SK 
			FROM	EDWCR.Ref_Rad_Onc_Site) rs1
	ON rs1.Site_Sk=ra1.Site_SK)ra1
	ON dp.DimCourseID = ra1.Source_Patient_Course_Id
	AND ra1.Source_Site_Id=dp.DimSiteID
 
LEFT OUTER JOIN 
(
SELECT	Source_Diagnosis_Code_Id,
			Diagnosis_Code_SK,Source_Site_Id 
FROM
(
		SELECT	Source_Diagnosis_Code_Id,
			Diagnosis_Code_SK,Site_SK 
		FROM	EDWCR.Ref_Rad_Onc_Diagnosis_Code) dc
		
		INNER JOIN (
			SELECT	Source_Site_Id,
					Site_SK 
			FROM	EDWCR.Ref_Rad_Onc_Site) rs2
	ON rs2.Site_Sk=dc.Site_SK)dc
	ON dp.DimDiagnosisCodeID = dc.Source_Diagnosis_Code_Id
	AND dc.Source_Site_Id=dp.DimSiteID)STG