
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEPERFRELGRP ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEPERFRELGRP (PSPERELGRPID, PSPERELGRPKEY, PSPERELGRPPERFID)
SELECT TRIM(source.PSPERELGRPID), source.PSPERELGRPKEY, TRIM(source.PSPERELGRPPERFID)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPEPERFRELGRP as source;
