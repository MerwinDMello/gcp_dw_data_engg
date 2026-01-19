
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgHCDCCATEGORY ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgHCDCCATEGORY (HCDCDESC, HCDCID)
SELECT TRIM(source.HCDCDESC), TRIM(source.HCDCID)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgHCDCCATEGORY as source;
