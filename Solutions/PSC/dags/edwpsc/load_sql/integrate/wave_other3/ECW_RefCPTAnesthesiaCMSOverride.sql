
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefCPTAnesthesiaCMSOverride ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefCPTAnesthesiaCMSOverride (CPTCode, CMSBaseUnit)
SELECT TRIM(source.CPTCode), source.CMSBaseUnit
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefCPTAnesthesiaCMSOverride as source;
