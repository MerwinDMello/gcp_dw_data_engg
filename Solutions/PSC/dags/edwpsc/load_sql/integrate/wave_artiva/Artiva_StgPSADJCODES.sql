
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSADJCODES ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSADJCODES (PSADACTIVE, PSADAPPRVLREQ, PSADDESC, PSADID, PSADSEC, PSADTYPE)
SELECT TRIM(source.PSADACTIVE), TRIM(source.PSADAPPRVLREQ), TRIM(source.PSADDESC), TRIM(source.PSADID), TRIM(source.PSADSEC), TRIM(source.PSADTYPE)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSADJCODES as source;
