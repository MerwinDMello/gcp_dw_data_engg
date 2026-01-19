SELECT 'J_CR_RO_Rad_Onc_Activity_Transaction'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
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
)STG