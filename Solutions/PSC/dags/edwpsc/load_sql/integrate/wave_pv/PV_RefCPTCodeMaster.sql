
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.PV_RefCPTCodeMaster ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.PV_RefCPTCodeMaster (Code, CodeDesc, CustomCodeFlag)
SELECT TRIM(source.Code), TRIM(source.CodeDesc), source.CustomCodeFlag
FROM {{ params.param_psc_stage_dataset_name }}.PV_RefCPTCodeMaster as source;
