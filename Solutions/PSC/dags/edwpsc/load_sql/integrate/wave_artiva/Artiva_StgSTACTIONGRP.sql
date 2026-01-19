
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgSTACTIONGRP ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgSTACTIONGRP (STACTGID, STACTGMSEC, STACTGSDSC, STACTGSER, STACTGTBL, STACTGTYP)
SELECT TRIM(source.STACTGID), source.STACTGMSEC, TRIM(source.STACTGSDSC), TRIM(source.STACTGSER), TRIM(source.STACTGTBL), TRIM(source.STACTGTYP)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgSTACTIONGRP as source;
