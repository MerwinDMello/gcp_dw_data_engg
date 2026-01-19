
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefFinancialPayorGroupSize ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefFinancialPayorGroupSize (RankNumber, FinancialPayorGroupName, FinancialPayorGroupSize, Charges13Month, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
SELECT source.RankNumber, TRIM(source.FinancialPayorGroupName), TRIM(source.FinancialPayorGroupSize), source.Charges13Month, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefFinancialPayorGroupSize as source;
