
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.EPIC_RefFinancialClassCrosswalk ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefFinancialClassCrosswalk (SourceSystemFinClassC, FinancialClassCode, RegionKey)
SELECT TRIM(source.SourceSystemFinClassC), TRIM(source.FinancialClassCode), source.RegionKey
FROM {{ params.param_psc_stage_dataset_name }}.EPIC_RefFinancialClassCrosswalk as source;
