
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgHCPERSON_BadAddress ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgHCPERSON_BadAddress (HCPNBADADR, PSPNBADADRDTE, PSPNCONTACTID)
SELECT TRIM(source.HCPNBADADR), source.PSPNBADADRDTE, TRIM(source.PSPNCONTACTID)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgHCPERSON_BadAddress as source;
