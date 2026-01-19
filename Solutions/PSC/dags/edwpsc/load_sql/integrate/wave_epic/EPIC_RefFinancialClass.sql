
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.EPIC_RefFinancialClass ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefFinancialClass (FinancialClassKey, FinancialClassName, FinancialClassIsPatientBalance, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
SELECT source.FinancialClassKey, TRIM(source.FinancialClassName), source.FinancialClassIsPatientBalance, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM
FROM {{ params.param_psc_stage_dataset_name }}.EPIC_RefFinancialClass as source;
