SELECT 'J_CR_RO_Fact_Rad_Onc_Patient_Toxicity'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM 
(SELECT
ROW_NUMBER() OVER (
ORDER BY DimSiteID, FactpatientToxicityID DESC) AS Fact_Patient_Toxicity_SK FROM	(
			SELECT	
			ToxicityComponentName,
			ToxicityAssessmentType,
			DimActivityTransactionID,
			DimPatientID,
			DimLookupID_Scheme,
		DimLookupID_ToxctyCseCrtntyTyp,
		DimLookupID_ToxicityCauseType,
		DimDiagnosisCodeID,
		DimSiteID,
		FactPatientToxicityID,
		AssessmentDateTime,
		ToxicityEffectiveDate,
		ToxicityGrade,
		ValidEntryIndicator,
		ToxicityApprovedDateTime,
		AssessmentPerformedDateTime,
		ToxicityReason,
		ToxicityApprovedIndicator,
		ToxctyHeaderValidEtryIndicator,
		RevisionNumber,
		LogID,
		RunID
		
			FROM	EDWCR_Staging.stg_FACTPATIENTTOXICITY) dp
INNER JOIN (
			SELECT	Source_Site_Id,
					Site_SK 
			FROM	EDWCR.Ref_Rad_Onc_Site) rr
	ON dp.DimSiteID = rr.Source_Site_Id
)STG