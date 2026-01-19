
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEPAYTAXEX ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEPAYTAXEX (PSPEPAYTXKEY, PSPEPAYTXPAYID, PSPEPAYTXTAX, PSPEPAYTXTAXOP, PSPEPAYTXTYP)
SELECT source.PSPEPAYTXKEY, TRIM(source.PSPEPAYTXPAYID), TRIM(source.PSPEPAYTXTAX), TRIM(source.PSPEPAYTXTAXOP), TRIM(source.PSPEPAYTXTYP)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPEPAYTAXEX as source;
