
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefRH1201ReleaseStatus ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefRH1201ReleaseStatus (RH1201ReleaseStatusKey, RH1201ReleaseStatusName, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag)
SELECT TRIM(source.RH1201ReleaseStatusKey), TRIM(source.RH1201ReleaseStatusName), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefRH1201ReleaseStatus as source;
