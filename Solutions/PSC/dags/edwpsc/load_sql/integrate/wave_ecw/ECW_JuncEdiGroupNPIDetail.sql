

TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_JuncEdiGroupNPIDetail ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncEdiGroupNPIDetail (ECWEdiGroupNPIDetailKey, RegionKey, NPIRuleId, ProviderId, ProviderKey, FacilityId, FacilityKey, deleteFlag, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
SELECT  ECWEdiGroupNPIDetailKey, RegionKey, NPIRuleId, ProviderId, ProviderKey, FacilityId, FacilityKey, deleteFlag, SourcePrimaryKeyValue, DWLastUpdateDateTime, TRIM(SourceSystemCode), TRIM(InsertedBy), InsertedDTM, TRIM(ModifiedBy), ModifiedDTM
FROM {{ params.param_psc_stage_dataset_name }}.ECW_JuncEdiGroupNPIDetail as source;