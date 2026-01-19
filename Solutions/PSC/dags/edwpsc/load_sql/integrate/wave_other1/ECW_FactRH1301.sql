
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_FactRH1301 ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_FactRH1301 (RH1301Key, ClaimKey, ClaimNumber, Coid, ImportDateKey, RH1301ClaimID, RH1301ClaimDateKey, RH1301TotalAmt, RH1301InsuranceBilledName, RH1301ErrorFieldName, RH1301ErrorIndex, RH1301ErrorData, RH1301ErrorDescription, RH1301StmtThruDateKey, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, FullClaimNumber, RegionKey)
SELECT source.RH1301Key, source.ClaimKey, source.ClaimNumber, TRIM(source.Coid), source.ImportDateKey, TRIM(source.RH1301ClaimID), source.RH1301ClaimDateKey, source.RH1301TotalAmt, TRIM(source.RH1301InsuranceBilledName), TRIM(source.RH1301ErrorFieldName), TRIM(source.RH1301ErrorIndex), TRIM(source.RH1301ErrorData), TRIM(source.RH1301ErrorDescription), source.RH1301StmtThruDateKey, source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.FullClaimNumber), source.RegionKey
FROM {{ params.param_psc_stage_dataset_name }}.ECW_FactRH1301 as source;
