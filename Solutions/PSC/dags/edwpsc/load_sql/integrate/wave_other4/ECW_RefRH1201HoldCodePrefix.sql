
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefRH1201HoldCodePrefix ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefRH1201HoldCodePrefix (RH1201HoldCodePrefixKey, RH1201HoldCodePrefixName, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag)
SELECT TRIM(source.RH1201HoldCodePrefixKey), TRIM(source.RH1201HoldCodePrefixName), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefRH1201HoldCodePrefix as source;
