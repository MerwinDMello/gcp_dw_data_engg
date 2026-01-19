
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefPlaceOfService ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefPlaceOfService (POSKey, POSCategoryKey, POSName, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, PaymentRate)
SELECT source.POSKey, source.POSCategoryKey, TRIM(source.POSName), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.PaymentRate)
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefPlaceOfService as source;
