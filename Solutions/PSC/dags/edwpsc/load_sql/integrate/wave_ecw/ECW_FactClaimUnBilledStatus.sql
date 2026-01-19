
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_FactClaimUnBilledStatus ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_FactClaimUnBilledStatus (ClaimKey, ClaimNumber, UnBilledStatusKey, ClaimUnBilledStatusInRHInventory, ClaimUnBilledStatusRHHoldCode, ClaimUnBilledStatusEdiNoHold, ClaimUnBilledStatusMinSubmissionDate, ClaimUnBilledStatusMaxSubmissionDate, ClaimUnBilledStatusClaimStatus, Coid, RegionKey, InsertedDTM, RhUnbilledCategory, HoldCategory, ClaimStatusOwner)
SELECT source.ClaimKey, source.ClaimNumber, source.UnBilledStatusKey, source.ClaimUnBilledStatusInRHInventory, TRIM(source.ClaimUnBilledStatusRHHoldCode), source.ClaimUnBilledStatusEdiNoHold, source.ClaimUnBilledStatusMinSubmissionDate, source.ClaimUnBilledStatusMaxSubmissionDate, TRIM(source.ClaimUnBilledStatusClaimStatus), TRIM(source.Coid), source.RegionKey, source.InsertedDTM, TRIM(source.RhUnbilledCategory), TRIM(source.HoldCategory), TRIM(source.ClaimStatusOwner)
FROM {{ params.param_psc_stage_dataset_name }}.ECW_FactClaimUnBilledStatus as source;
