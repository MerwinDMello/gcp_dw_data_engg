
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.PV_FactRH1301 ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.PV_FactRH1301 (RH1301Key, ClaimKey, ClaimNumber, RegionKey, Coid, ImportDateKey, RH1301ClaimID, RH1301ClaimDateKey, RH1301TotalAmt, RH1301InsuranceBilledName, RH1301ErrorFieldName, RH1301ErrorIndex, RH1301ErrorData, RH1301ErrorDescription, RH1301StmtThruDateKey, RH1301FileName, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
SELECT source.RH1301Key, source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.Coid), source.ImportDateKey, TRIM(source.RH1301ClaimID), source.RH1301ClaimDateKey, source.RH1301TotalAmt, TRIM(source.RH1301InsuranceBilledName), TRIM(source.RH1301ErrorFieldName), TRIM(source.RH1301ErrorIndex), TRIM(source.RH1301ErrorData), TRIM(source.RH1301ErrorDescription), source.RH1301StmtThruDateKey, TRIM(source.RH1301FileName), source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM
FROM {{ params.param_psc_stage_dataset_name }}.PV_FactRH1301 as source;
