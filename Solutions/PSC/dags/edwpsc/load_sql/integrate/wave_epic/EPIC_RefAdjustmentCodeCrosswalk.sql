
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.EPIC_RefAdjustmentCodeCrosswalk ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefAdjustmentCodeCrosswalk (AdjCode, RegionKey, AdjustmentCategoryKey, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
SELECT TRIM(source.AdjCode), source.RegionKey, source.AdjustmentCategoryKey, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM
FROM {{ params.param_psc_stage_dataset_name }}.EPIC_RefAdjustmentCodeCrosswalk as source;
