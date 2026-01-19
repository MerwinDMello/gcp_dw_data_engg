
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimUnBilledStatus ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimUnBilledStatus (ClaimKey, ClaimNumber, RegionKey, Coid, PatientInternalID, VisitNumber, UnBilledStatusKey, ClaimUnBilledStatusInRHInventory, ClaimUnBilledStatusRHHoldCode, ClaimUnBilledStatusEdiNoHold, ClaimUnBilledStatusMinSubmissionDate, ClaimUnBilledStatusMaxSubmissionDate, ClaimUnBilledStatusClaimStatus, InsertedDTM, DWLastUpdateDateTime)
SELECT source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.Coid), source.PatientInternalID, source.VisitNumber, source.UnBilledStatusKey, source.ClaimUnBilledStatusInRHInventory, TRIM(source.ClaimUnBilledStatusRHHoldCode), source.ClaimUnBilledStatusEdiNoHold, source.ClaimUnBilledStatusMinSubmissionDate, source.ClaimUnBilledStatusMaxSubmissionDate, TRIM(source.ClaimUnBilledStatusClaimStatus), source.InsertedDTM, source.DWLastUpdateDateTime
FROM {{ params.param_psc_stage_dataset_name }}.EPIC_FactClaimUnBilledStatus as source;
