TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_JuncProviderCoid ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncProviderCoid (JuncProviderCoid, ProviderKey, Coid, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ProviderType, DWLastUpdateDateTime)
SELECT source.JuncProviderCoid, source.ProviderKey, TRIM(source.Coid), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.ProviderType, source.DWLastUpdateDateTime
FROM {{ params.param_psc_stage_dataset_name }}.ECW_JuncProviderCoid as source;