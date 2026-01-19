
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgSTWORKFLOW ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgSTWORKFLOW (STWKFCONV, STWKFID, STWKFINIT, STWKFLDSC, STWKFMSEC, STWKFPARNT, STWKFSDSC, STWKFTBL)
SELECT TRIM(source.STWKFCONV), TRIM(source.STWKFID), TRIM(source.STWKFINIT), TRIM(source.STWKFLDSC), source.STWKFMSEC, TRIM(source.STWKFPARNT), TRIM(source.STWKFSDSC), TRIM(source.STWKFTBL)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgSTWORKFLOW as source;
