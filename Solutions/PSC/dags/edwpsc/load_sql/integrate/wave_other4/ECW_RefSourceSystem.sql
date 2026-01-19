
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefSourceSystem ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefSourceSystem (SourceSystemKey, SourceSystemCode, SourceSystemShortName, SourceSystemLongName, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
SELECT source.SourceSystemKey, TRIM(source.SourceSystemCode), TRIM(source.SourceSystemShortName), TRIM(source.SourceSystemLongName), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefSourceSystem as source;
