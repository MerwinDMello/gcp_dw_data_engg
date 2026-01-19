
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSAPPRVLTABLES ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSAPPRVLTABLES (PSTADESC, PSTANAME, PSTAREQUESTROLE)
SELECT TRIM(source.PSTADESC), TRIM(source.PSTANAME), TRIM(source.PSTAREQUESTROLE)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSAPPRVLTABLES as source;
