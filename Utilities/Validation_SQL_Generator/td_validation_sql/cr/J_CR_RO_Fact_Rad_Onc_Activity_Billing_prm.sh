
export JOBNAME='J_CR_RO_Fact_Rad_Onc_Activity_Billing'

export AC_EXP_SQL_STATEMENT="SELECT 'J_CR_RO_Fact_Rad_Onc_Activity_Billing'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM 
(SELECT
ROW_NUMBER() OVER (
ORDER BY DimSiteID,
		FactActivityBillingID DESC) AS Fact_Activity_Billing_SK
FROM	(
	SELECT	
	DimDoctorID,
	DimDoctID_AttdOncologist,
	DimCourseID,
	DimHospitalDepartmentID,
	DimActivityID,
	DimActivityTransactionID,
	DimProcedureCodeID,
	DimPatientID,
	DimLkpID_ActivityCategory,
	DimSiteID,
	FactActivityBillingID,
	PrimaryGlobalCharge,
	PrimaryTechnicalCharge,
	PrimaryProfessionalCharge,
	OtherProfessionalCharge,
	OtherTechnicalCharge,
	OtherGlobalCharge,
	ChargeForecast,
	ActualCharge,
	ActivityCost,
	TRIM(AccountBillingCode) AccountBillingCode,
	FromDateOfService,
	ToDateOfService,
	CompletedDateTime,
	ExportedDateTime,
	MarkedCompletedDateTime,
	CreditExportedDateTime,
	CreditedDateTime,
	TRIM(CreditNote) CreditNote,
	TRIM(AllModifierCodes) AllModifierCodes,
	CreditAmount,
	IsScheduled,
	ObjectStatus,
	LogID,
	RunID
	
	FROM	EDWCR_Staging.FACTACTIVITYBILLING_STG) dp

INNER JOIN (
	SELECT	Source_Site_Id,
			Site_SK 
	FROM	EDWCR.Ref_Rad_Onc_Site) rr

	ON dp.DimSiteID = rr.Source_Site_Id

)STG"

export AC_ACT_SQL_STATEMENT="select 'J_CR_RO_Fact_Rad_Onc_Activity_Billing'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.Fact_Rad_Onc_Activity_Billing
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM ${NCR_AC_SCHEMA}.ETL_JOB_RUN where Job_Name = '$JOBNAME')
"


