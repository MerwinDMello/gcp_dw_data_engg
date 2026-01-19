
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefPlaceOfServiceCategory ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefPlaceOfServiceCategory (POSCategoryKey, POSCategoryName, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
SELECT source.POSCategoryKey, TRIM(source.POSCategoryName), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefPlaceOfServiceCategory as source;
