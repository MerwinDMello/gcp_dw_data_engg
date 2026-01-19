
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgGCPOOL ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgGCPOOL (gcpoolnum, gcpoolact)
SELECT TRIM(source.gcpoolnum), TRIM(source.gcpoolact)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgGCPOOL as source;
