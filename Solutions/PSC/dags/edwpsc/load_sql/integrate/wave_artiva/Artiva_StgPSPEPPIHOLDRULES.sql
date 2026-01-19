
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEPPIHOLDRULES ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEPPIHOLDRULES (PSPEPPIHRKEY, PSPEPPIHRPPIID, PSPEPPIHRUID)
SELECT source.PSPEPPIHRKEY, TRIM(source.PSPEPPIHRPPIID), source.PSPEPPIHRUID
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPEPPIHOLDRULES as source;
