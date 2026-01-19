
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEPAYLINKGROUPS ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEPAYLINKGROUPS (PPSPEPAYLKGRPDESC, PSPEPAYLKGRPKEY)
SELECT TRIM(source.PPSPEPAYLKGRPDESC), TRIM(source.PSPEPAYLKGRPKEY)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPEPAYLINKGROUPS as source;
