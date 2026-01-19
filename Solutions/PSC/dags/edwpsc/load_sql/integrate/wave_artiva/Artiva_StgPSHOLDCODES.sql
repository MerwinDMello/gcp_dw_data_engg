
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSHOLDCODES ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSHOLDCODES (PSHCDESC, PSHCID, PSHCMTYPE, PSHCPRIORITY, PSHCTYPE, PSHCRISKREPORT)
SELECT TRIM(source.PSHCDESC), TRIM(source.PSHCID), TRIM(source.PSHCMTYPE), source.PSHCPRIORITY, TRIM(source.PSHCTYPE), TRIM(source.PSHCRISKREPORT)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSHOLDCODES as source;
