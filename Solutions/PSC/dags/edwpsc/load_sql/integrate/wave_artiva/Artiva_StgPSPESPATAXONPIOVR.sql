
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPESPATAXONPIOVR ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPESPATAXONPIOVR (PSPESTNKEY, PSPESTNNPI, PSPESTNSPAID, PSPESTNTAXONOMY)
SELECT TRIM(source.PSPESTNKEY), TRIM(source.PSPESTNNPI), source.PSPESTNSPAID, TRIM(source.PSPESTNTAXONOMY)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPESPATAXONPIOVR as source;
