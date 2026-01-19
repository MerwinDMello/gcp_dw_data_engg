
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEPAYCPID ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEPAYCPID (PSPEPAYCPCPIDID, PSPEPAYCPKEY, PSPEPAYCPPAYID)
SELECT TRIM(source.PSPEPAYCPCPIDID), source.PSPEPAYCPKEY, TRIM(source.PSPEPAYCPPAYID)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPEPAYCPID as source;
