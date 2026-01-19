
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.EPIC_FactWorkQueuePatientErrors ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactWorkQueuePatientErrors (RegionKey, CerId, RuleId, ErrorMessage, DeferDate, SourceAPrimaryKeyValue, SourceBPrimaryKeyValue, InsertedBy, InsertedDTM, DWLastUpdateDateTime)
SELECT source.RegionKey, source.CerId, source.RuleId, TRIM(source.ErrorMessage), source.DeferDate, source.SourceAPrimaryKeyValue, source.SourceBPrimaryKeyValue, TRIM(source.InsertedBy), source.InsertedDTM, source.DWLastUpdateDateTime
FROM {{ params.param_psc_stage_dataset_name }}.EPIC_FactWorkQueuePatientErrors as source;
