
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.PV_FactRH1201 ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.PV_FactRH1201 (RH1201Key, ImportDateKey, ClaimKey, ClaimNumber, RegionKey, Coid, RH1201ClaimID, RH1201InsuranceBilledName, RH1201BillStatusCode, RH1201BillClaimStatusKey, RH1201ReleaseStatusKey, RH1201TypeOfBill, RH1201ClaimDateKey, RH1201StmtThruDateKey, RH1201TotalAmt, RH1201UserID, RH1201HoldCode, RH1201HoldCodePrefixKey, RH1201FileName, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
SELECT source.RH1201Key, source.ImportDateKey, source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.Coid), TRIM(source.RH1201ClaimID), TRIM(source.RH1201InsuranceBilledName), TRIM(source.RH1201BillStatusCode), TRIM(source.RH1201BillClaimStatusKey), TRIM(source.RH1201ReleaseStatusKey), TRIM(source.RH1201TypeOfBill), source.RH1201ClaimDateKey, source.RH1201StmtThruDateKey, source.RH1201TotalAmt, TRIM(source.RH1201UserID), TRIM(source.RH1201HoldCode), TRIM(source.RH1201HoldCodePrefixKey), TRIM(source.RH1201FileName), source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM
FROM {{ params.param_psc_stage_dataset_name }}.PV_FactRH1201 as source;
