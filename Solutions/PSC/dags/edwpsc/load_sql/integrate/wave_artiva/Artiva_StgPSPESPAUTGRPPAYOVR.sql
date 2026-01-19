
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPESPAUTGRPPAYOVR ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPESPAUTGRPPAYOVR (PSPESPAUTGPOVKEY, PSPESPAUTGPOVPAYID, PSPESPAUTGPOVSPAID)
SELECT TRIM(source.PSPESPAUTGPOVKEY), TRIM(source.PSPESPAUTGPOVPAYID), source.PSPESPAUTGPOVSPAID
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPESPAUTGRPPAYOVR as source;
