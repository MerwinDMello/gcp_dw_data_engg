
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.PK_FactHoldingBinSummary ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.PK_FactHoldingBinSummary (HoldingBinSummaryKey, PKRegionName, COID, BillingAreaName, BillingProviderFirstName, BillingProviderLastName, BillingProviderUserName, PatientMRN, PatientFirstName, PatientLastName, PKFinancialClass, ServiceDate, SubmissionDate, LastReviewedDate, LastSavedDate, VisitType, ChargeRVU, ResponsibleParty, HoldingBinCategory, HoldingBinSubCategory, CPTCount, SourceAPrimaryKeyValue, InsertedBy, InsertedDTM, PracticeId)
SELECT source.HoldingBinSummaryKey, TRIM(source.PKRegionName), TRIM(source.COID), TRIM(source.BillingAreaName), TRIM(source.BillingProviderFirstName), TRIM(source.BillingProviderLastName), TRIM(source.BillingProviderUserName), TRIM(source.PatientMRN), TRIM(source.PatientFirstName), TRIM(source.PatientLastName), TRIM(source.PKFinancialClass), source.ServiceDate, source.SubmissionDate, source.LastReviewedDate, source.LastSavedDate, TRIM(source.VisitType), TRIM(source.ChargeRVU), TRIM(source.ResponsibleParty), TRIM(source.HoldingBinCategory), TRIM(source.HoldingBinSubCategory), source.CPTCount, TRIM(source.SourceAPrimaryKeyValue), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.PracticeId)
FROM {{ params.param_psc_stage_dataset_name }}.PK_FactHoldingBinSummary as source;
