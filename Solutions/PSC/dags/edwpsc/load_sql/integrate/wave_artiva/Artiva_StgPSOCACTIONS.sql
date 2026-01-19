
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSOCACTIONS ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSOCACTIONS (PSOCAKEY, PSOCANAME)
SELECT TRIM(source.PSOCAKEY), TRIM(source.PSOCANAME)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSOCACTIONS as source;
