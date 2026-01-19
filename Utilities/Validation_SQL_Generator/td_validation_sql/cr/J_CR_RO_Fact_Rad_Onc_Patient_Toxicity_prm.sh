
export JOBNAME='J_CR_RO_Fact_Rad_Onc_Patient_Toxicity'

export AC_EXP_SQL_STATEMENT="SELECT 'J_CR_RO_Fact_Rad_Onc_Patient_Toxicity'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
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

)STG"

export AC_ACT_SQL_STATEMENT="select 'J_CR_RO_Fact_Rad_Onc_Patient_Toxicity'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.Fact_Rad_Onc_Patient_Toxicity
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM ${NCR_AC_SCHEMA}.ETL_JOB_RUN where Job_Name = '$JOBNAME')
"


