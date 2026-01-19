
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.CCU_RefMatchType ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.CCU_RefMatchType (MatchTypeKey, MatchType, MatchFlag, MatchTypeDesc, MatchTypeLongDesc)
SELECT source.MatchTypeKey, TRIM(source.MatchType), TRIM(source.MatchFlag), TRIM(source.MatchTypeDesc), TRIM(source.MatchTypeLongDesc)
FROM {{ params.param_psc_stage_dataset_name }}.CCU_RefMatchType as source;
