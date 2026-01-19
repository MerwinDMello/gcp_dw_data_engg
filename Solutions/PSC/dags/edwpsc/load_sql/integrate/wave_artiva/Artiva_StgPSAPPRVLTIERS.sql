
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSAPPRVLTIERS ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSAPPRVLTIERS (PSATID, PSATLEVEL, PSATMAXIMUM, PSATMINIMUM, PSATROLE, PSATTABLEID)
SELECT source.PSATID, source.PSATLEVEL, source.PSATMAXIMUM, source.PSATMINIMUM, TRIM(source.PSATROLE), TRIM(source.PSATTABLEID)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSAPPRVLTIERS as source;
