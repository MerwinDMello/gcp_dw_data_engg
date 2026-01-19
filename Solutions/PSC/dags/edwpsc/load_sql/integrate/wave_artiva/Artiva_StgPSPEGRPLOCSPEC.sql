
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEGRPLOCSPEC ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEGRPLOCSPEC (PSPEGLSGLID, PSPEGLSKEY, PSPEGLSSPECNAME, PSPEGLSTAXONOMY)
SELECT TRIM(source.PSPEGLSGLID), TRIM(source.PSPEGLSKEY), TRIM(source.PSPEGLSSPECNAME), TRIM(source.PSPEGLSTAXONOMY)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPEGRPLOCSPEC as source;
