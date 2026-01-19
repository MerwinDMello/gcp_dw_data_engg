
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSOCROUTESTEPS ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSOCROUTESTEPS (PSOCRKEY, PSOCRNAME)
SELECT TRIM(source.PSOCRKEY), TRIM(source.PSOCRNAME)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSOCROUTESTEPS as source;
