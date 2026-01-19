
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEGRPLOCECWFAC ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEGRPLOCECWFAC (PSPEGRPLFECWFACID, PSPEGRPLFGLID, PSPEGRPLFKEY)
SELECT TRIM(source.PSPEGRPLFECWFACID), TRIM(source.PSPEGRPLFGLID), TRIM(source.PSPEGRPLFKEY)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPEGRPLOCECWFAC as source;
