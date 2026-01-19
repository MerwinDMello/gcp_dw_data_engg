
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgSTPHASE ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgSTPHASE (STPHSAVL, STPHSID, STPHSLDSC, STPHSMSEC, STPHSPOS, STPHSSDSC, STPHSTBL, STPHSTYP, STPHSWKF)
SELECT TRIM(source.STPHSAVL), TRIM(source.STPHSID), TRIM(source.STPHSLDSC), source.STPHSMSEC, source.STPHSPOS, TRIM(source.STPHSSDSC), TRIM(source.STPHSTBL), TRIM(source.STPHSTYP), TRIM(source.STPHSWKF)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgSTPHASE as source;
