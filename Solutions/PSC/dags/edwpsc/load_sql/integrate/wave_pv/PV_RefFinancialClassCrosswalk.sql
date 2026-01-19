
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.PV_RefFinancialClassCrosswalk ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.PV_RefFinancialClassCrosswalk (FinancialClassKey, SourceFinancialClass, IsPatientFlag, DWLastUpdateDateTime, SourceSystemCode)
SELECT source.FinancialClassKey, TRIM(source.SourceFinancialClass), source.IsPatientFlag, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode)
FROM {{ params.param_psc_stage_dataset_name }}.PV_RefFinancialClassCrosswalk as source;
