
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEPAYLEGACY ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEPAYLEGACY (PSPEPAYLPKEY, PSPEPAYLPPAYID, PSPEPAYLPREFPAYID)
SELECT source.PSPEPAYLPKEY, TRIM(source.PSPEPAYLPPAYID), TRIM(source.PSPEPAYLPREFPAYID)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPEPAYLEGACY as source;
