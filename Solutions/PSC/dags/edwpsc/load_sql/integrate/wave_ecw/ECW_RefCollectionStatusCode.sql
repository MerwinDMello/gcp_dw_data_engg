
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefCollectionStatusCode ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefCollectionStatusCode (CollectionStatusCode, CollectionStatusDesc)
SELECT TRIM(source.CollectionStatusCode), TRIM(source.CollectionStatusDesc)
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefCollectionStatusCode as source;
