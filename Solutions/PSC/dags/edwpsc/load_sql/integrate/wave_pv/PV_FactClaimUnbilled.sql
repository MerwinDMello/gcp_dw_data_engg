
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.PV_FactClaimUnbilled ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.PV_FactClaimUnbilled (RegionKey, Coid, Practice, ClaimKey, ClaimNumber, UnbilledStatusKey, ClaimUnbilledStatusInRHInventory, ClaimUnbilledStatusRHHoldCode, ClaimUnbilledStatusEdiNoHold, ClaimUnbilledStatusMinSubmissionDate, ClaimUnbilledStatusMaxSubmissionDate, ClaimUnbilledStatusClaimStatus, InsertedBy, InsertedDTM, RhUnbilledCategory, ClaimStatusOwner, DWLastUpdateDateTime)
SELECT source.RegionKey, TRIM(source.Coid), TRIM(source.Practice), source.ClaimKey, source.ClaimNumber, source.UnbilledStatusKey, source.ClaimUnbilledStatusInRHInventory, TRIM(source.ClaimUnbilledStatusRHHoldCode), source.ClaimUnbilledStatusEdiNoHold, source.ClaimUnbilledStatusMinSubmissionDate, source.ClaimUnbilledStatusMaxSubmissionDate, TRIM(source.ClaimUnbilledStatusClaimStatus), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.RhUnbilledCategory), TRIM(source.ClaimStatusOwner), source.DWLastUpdateDateTime
FROM {{ params.param_psc_stage_dataset_name }}.PV_FactClaimUnbilled as source;
