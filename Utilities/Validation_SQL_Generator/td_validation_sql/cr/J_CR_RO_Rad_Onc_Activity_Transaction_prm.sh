
export JOBNAME='J_CR_RO_Rad_Onc_Activity_Transaction'

export AC_EXP_SQL_STATEMENT="SELECT 'J_CR_RO_Rad_Onc_Activity_Transaction'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM 
(SELECT
ROW_NUMBER() OVER (
ORDER BY DimSiteID,DimActivityID DESC) AS Fact_Patient_Toxicity_SK FROM	(
							SELECT	
							DimSiteID,
							DimActivityID,
							ActivityPriority,
							DimHospitalDepartmentID,
							DimPatientID,
							DimLookupID_AppointmentStatus,
				DimLookupID_ActualResourceType,
				DimLookupID_CancelReasonType,
				DimLookupID_AppointmentRsrcSta,
				DimActivityTransactionID,
				ScheduledEndTime,
				AppointmentDateTime,
				IsScheduled,
				ActivityStartDateTime,
				ActivityEndDateTime,
				TRIM(ActivityNote) AS ActivityNote,
				CheckedIn,
				PatientArrivalDateTime,
				WaitListedFlag,
				TRIM(PatientLocation) AS PatientLocation,
				AppointmentInstanceFlag,
				DerivedAppointmentTaskDate,
				ActivityOwnerFlag,
				IsVisitTypeOpenChart,
				ctrResourceSer,
				LogID,
				RunID
				
						
							FROM	EDWCR_Staging.stg_DimActivityTransaction) dp

INNER JOIN (
							SELECT	Source_Site_Id,
									Site_SK 
							FROM	EDWCR.Ref_Rad_Onc_Site) rr

	ON dp.DimSiteID = rr.Source_Site_Id
)STG"

export AC_ACT_SQL_STATEMENT="select 'J_CR_RO_Rad_Onc_Activity_Transaction'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.Rad_Onc_Activity_Transaction
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM ${NCR_AC_SCHEMA}.ETL_JOB_RUN where Job_Name = '$JOBNAME')
"


