
export JOBNAME='J_CR_RO_Fact_rad_Onc_Treatment_History'

export AC_EXP_SQL_STATEMENT="SELECT 'J_CR_RO_Fact_rad_Onc_Treatment_History'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM 
(select	
ROW_NUMBER() OVER ( 
ORDER BY CAST(DimSiteID AS INT),
		CAST(FactTreatmentHistoryID AS INT)) AS Fact_Treatment_History_SK,

ra.Patient_Course_SK as Patient_Course_SK,
pp.Patient_Plan_SK  as Patient_Plan_SK,
ra1.Patient_SK as Patient_SK,
DimLkpID_TreatmentIntentType  ,
DimLookupID_ClinicalStatus    ,
DimLookupID_PlanStatus,
DimLookupID_FieldTechnique,
DimLookupID_Technique,
DimLookupID_TechniqueLabel,
DimLkpID_TreatmentDeliveryTyp ,
rr.Site_SK as Site_SK,
FactTreatmentHistoryID,
Completion_Date_Time,
First_Treatment_Date_Time,
Last_Treatment_Date_Time,
Status_Date_Time,
Active_Ind,
PlannedDoseRate,
CourseDoseDelivered,
CourseDosePlanned,
CourseDoseRemaining,
OtherCourseDoseDelivered,
DoseCorrection,
TotalDoseLimit,
DailyDoseLimit,
SessionDoseLimit,
Primary_Ind,
LogID,
RunID,
'R' AS Source_System_Code,
 CURRENT_TIMESTAMP (0)  AS DW_Last_Update_Date_Time
From	

(
	select	 
	DimSiteID,
	DimCourseID,
	DimPlanID,
	DimPatientID,
	DimLkpID_TreatmentIntentType  ,
	DimLookupID_ClinicalStatus    ,
	DimLookupID_PlanStatus,
	DimLookupID_FieldTechnique,
	DimLookupID_Technique,
	DimLookupID_TechniqueLabel,
	DimLkpID_TreatmentDeliveryTyp ,
	FactTreatmentHistoryID,
	CAST(TRIM(CompletedDateTime) AS TIMESTAMP(0))  AS Completion_Date_Time,
	CAST (TRIM(FirstTreatmentDate) AS TIMESTAMP(0)) AS First_Treatment_Date_Time,
	CAST(TRIM(LastTreatmentDate) AS TIMESTAMP(0))  AS Last_Treatment_Date_Time,
	CAST(TRIM(StatusDate) as timestamp(0)) as Status_Date_Time,
			CASE 
				WHEN IsActive =1 THEN 'Y'
				WHEN IsActive=0 THEN 'N'
			END AS Active_Ind,
	PlannedDoseRate,
	TRIM(CourseDoseDelivered) CourseDoseDelivered,
	TRIM(CourseDosePlanned) CourseDosePlanned,
	TRIM(CourseDoseRemaining) CourseDoseRemaining,
	TRIM(OtherCourseDoseDelivered) OtherCourseDoseDelivered,
	TRIM(DoseCorrection) DoseCorrection,
	TRIM(TotalDoseLimit) TotalDoseLimit,
	TRIM(DailyDoseLimit) DailyDoseLimit,
	TRIM(SessionDoseLimit) SessionDoseLimit,
	
			CASE 
				WHEN PrimaryFlag =1 THEN 'Y'
				WHEN PrimaryFlag=0 THEN 'N'
			END AS Primary_Ind,
	LogID,
	RunID
	from	EDWCR_Staging.FactTreatmentHist_STG)  dp 
inner join 

(
	select	Source_Site_Id,Site_SK 
	from	EDWCR_BASE_VIEWS.Ref_Rad_Onc_Site) rr 
	on  rr.Source_Site_Id=dp.DimSiteID

left outer join 
(
	select	(Cast(Patient_Course_SK as INT))  as Patient_Course_SK,
			Source_Patient_Course_Id,
			Site_SK  
	from	EDWCR_BASE_VIEWS.Rad_Onc_Patient_Course ) ra 
	on dp.DimCourseID = ra.Source_Patient_Course_Id
	and rr.Site_Sk=ra.Site_SK

left outer join 

( 
	select	Source_Patient_Plan_Id,
			Site_SK,Patient_Plan_SK 
	from	EDWCR_BASE_VIEWS.Rad_Onc_patient_plan) pp
		
	on dp.DimPlanID = pp.Source_Patient_Plan_Id	
	and rr.Site_Sk=pp.Site_SK
	
left outer join 
(
	select	Source_Patient_Id,
			Site_Sk,Patient_SK 
	from	EDWCR_BASE_VIEWS.Rad_Onc_patient ) ra1 
	on 
dp.DimPatientID = ra1.Source_Patient_Id
	AND rr.Site_Sk=ra1.Site_SK
)stg"

export AC_ACT_SQL_STATEMENT="select 'J_CR_RO_Fact_rad_Onc_Treatment_History'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.Fact_rad_Onc_Treatment_History
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM ${NCR_AC_SCHEMA}.ETL_JOB_RUN where Job_Name = '$JOBNAME')
"


