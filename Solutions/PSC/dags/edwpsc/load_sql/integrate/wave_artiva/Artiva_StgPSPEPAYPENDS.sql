
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEPAYPENDS ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEPAYPENDS (PSPEPAYPDKEY, PSPEPAYPDPAYID, PSPEPAYPDREFPAYID)
SELECT source.PSPEPAYPDKEY, TRIM(source.PSPEPAYPDPAYID), TRIM(source.PSPEPAYPDREFPAYID)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPEPAYPENDS as source;
