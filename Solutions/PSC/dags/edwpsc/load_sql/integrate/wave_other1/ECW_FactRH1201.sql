
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_FactRH1201 ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_FactRH1201 (RH1201Key, ImportDateKey, ClaimKey, ClaimNumber, Coid, RH1201ClaimID, RH1201InsuranceBilledName, RH1201BillStatusCode, RH1201BillClaimStatusKey, RH1201ReleaseStatusKey, RH1201TypeOfBill, RH1201ClaimDateKey, RH1201StmtThruDateKey, RH1201TotalAmt, RH1201UserID, RH1201HoldCode, RH1201HoldCodePrefixKey, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, FullClaimNumber, RegionKey)
SELECT source.RH1201Key, source.ImportDateKey, source.ClaimKey, source.ClaimNumber, TRIM(source.Coid), TRIM(source.RH1201ClaimID), TRIM(source.RH1201InsuranceBilledName), TRIM(source.RH1201BillStatusCode), TRIM(source.RH1201BillClaimStatusKey), TRIM(source.RH1201ReleaseStatusKey), TRIM(source.RH1201TypeOfBill), source.RH1201ClaimDateKey, source.RH1201StmtThruDateKey, source.RH1201TotalAmt, TRIM(source.RH1201UserID), TRIM(source.RH1201HoldCode), TRIM(source.RH1201HoldCodePrefixKey), source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.FullClaimNumber), source.RegionKey
FROM {{ params.param_psc_stage_dataset_name }}.ECW_FactRH1201 as source;
