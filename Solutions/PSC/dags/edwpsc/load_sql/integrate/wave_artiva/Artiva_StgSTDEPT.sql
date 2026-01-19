
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgSTDEPT ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgSTDEPT (STDEPTID, PSDEPTDIR, PSDEPTEXEC, PSDEPTMGR, STDEPTDESC)
SELECT TRIM(source.STDEPTID), TRIM(source.PSDEPTDIR), TRIM(source.PSDEPTEXEC), TRIM(source.PSDEPTMGR), TRIM(source.STDEPTDESC)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgSTDEPT as source;
