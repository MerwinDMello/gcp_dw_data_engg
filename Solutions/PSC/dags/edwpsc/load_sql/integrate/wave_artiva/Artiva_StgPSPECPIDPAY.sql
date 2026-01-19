
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPECPIDPAY ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPECPIDPAY (PSPECPKEY, PSPECPCPIDID, PSPECPPAYID)
SELECT source.PSPECPKEY, TRIM(source.PSPECPCPIDID), TRIM(source.PSPECPPAYID)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPECPIDPAY as source;
