
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefEncounterType ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefEncounterType (EncounterTypeKey, EncType, EncTypeDescription)
SELECT source.EncounterTypeKey, source.EncType, TRIM(source.EncTypeDescription)
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefEncounterType as source;
