
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPESPATMPPAYOVR ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPESPATMPPAYOVR (PSPESPAYOVRKEY, PSPESPAYOVRLOCFLG, PSPESPAYOVRPERFID, PSPESPAYOVRPID)
SELECT TRIM(source.PSPESPAYOVRKEY), TRIM(source.PSPESPAYOVRLOCFLG), TRIM(source.PSPESPAYOVRPERFID), TRIM(source.PSPESPAYOVRPID)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPESPATMPPAYOVR as source;
