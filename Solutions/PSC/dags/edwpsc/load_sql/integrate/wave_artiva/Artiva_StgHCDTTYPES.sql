
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgHCDTTYPES ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgHCDTTYPES (HCDTDESC, HCDTID)
SELECT TRIM(source.HCDTDESC), TRIM(source.HCDTID)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgHCDTTYPES as source;
