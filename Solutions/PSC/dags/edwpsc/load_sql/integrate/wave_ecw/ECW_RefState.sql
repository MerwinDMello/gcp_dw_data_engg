
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefState ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefState (StateKey, StateName, StateCapitalCity, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
SELECT TRIM(source.StateKey), TRIM(source.StateName), TRIM(source.StateCapitalCity), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefState as source;
