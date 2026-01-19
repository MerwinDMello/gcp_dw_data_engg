SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM
 (SELECT ROW_NUMBER() OVER (
 ORDER BY DimSiteID,
 FactActivityBillingID DESC) AS Fact_Activity_Billing_SK
 FROM
 (SELECT DimDoctorID,
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
 FROM EDWCR_Staging.FACTACTIVITYBILLING_STG) dp
 INNER JOIN
 (SELECT Source_Site_Id,
 Site_SK
 FROM EDWCR.Ref_Rad_Onc_Site) rr ON dp.DimSiteID = rr.Source_Site_Id)STG