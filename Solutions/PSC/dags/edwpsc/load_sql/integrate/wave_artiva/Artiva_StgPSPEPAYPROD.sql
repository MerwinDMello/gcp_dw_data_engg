
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEPAYPROD ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEPAYPROD (PSPEPAYPRDESC, PSPEPAYPRFINCLASS, PSPEPAYPRKEY, PSPEPAYPRNAME, PSPEPAYPRPAYID)
SELECT TRIM(source.PSPEPAYPRDESC), TRIM(source.PSPEPAYPRFINCLASS), source.PSPEPAYPRKEY, TRIM(source.PSPEPAYPRNAME), TRIM(source.PSPEPAYPRPAYID)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPEPAYPROD as source;
