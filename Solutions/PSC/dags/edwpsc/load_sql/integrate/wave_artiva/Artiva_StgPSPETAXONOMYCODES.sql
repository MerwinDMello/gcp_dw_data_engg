
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPETAXONOMYCODES ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPETAXONOMYCODES (PSPETAXDESC, PSPETAXKEY, PSPETAXMCRSPCCODE, PSPETAXMCRSUPTYPDESC)
SELECT TRIM(source.PSPETAXDESC), TRIM(source.PSPETAXKEY), TRIM(source.PSPETAXMCRSPCCODE), TRIM(source.PSPETAXMCRSUPTYPDESC)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPETAXONOMYCODES as source;
