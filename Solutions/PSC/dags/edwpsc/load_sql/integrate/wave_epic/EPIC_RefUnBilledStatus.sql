
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.EPIC_RefUnBilledStatus ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefUnBilledStatus (UnBilledStatusKey, UnBilledStatusCategory, UnBilledStatusSubCategory, UnBilledOnHoldFlag, UnBilledUnbilledFlag, UnBilledBilledFlag, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag)
SELECT source.UnBilledStatusKey, TRIM(source.UnBilledStatusCategory), TRIM(source.UnBilledStatusSubCategory), source.UnBilledOnHoldFlag, source.UnBilledUnbilledFlag, source.UnBilledBilledFlag, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag
FROM {{ params.param_psc_stage_dataset_name }}.EPIC_RefUnBilledStatus as source;
