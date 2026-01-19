
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgHCCLCATEGORY ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgHCCLCATEGORY (HCCADESC, HCCAID)
SELECT TRIM(source.HCCADESC), TRIM(source.HCCAID)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgHCCLCATEGORY as source;
