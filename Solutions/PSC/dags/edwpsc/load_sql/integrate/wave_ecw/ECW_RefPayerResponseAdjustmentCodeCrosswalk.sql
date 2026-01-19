
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefPayerResponseAdjustmentCodeCrosswalk ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefPayerResponseAdjustmentCodeCrosswalk (AdjCode, DenialCategory, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
SELECT TRIM(source.AdjCode), TRIM(source.DenialCategory), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefPayerResponseAdjustmentCodeCrosswalk as source;
