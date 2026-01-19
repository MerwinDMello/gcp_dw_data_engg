
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPESPAUSERS ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPESPAUSERS (PSPESPARELUKEY, PSPESPARELUSER, PSPESPARELUSPAID)
SELECT source.PSPESPARELUKEY, TRIM(source.PSPESPARELUSER), source.PSPESPARELUSPAID
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPESPAUSERS as source;
