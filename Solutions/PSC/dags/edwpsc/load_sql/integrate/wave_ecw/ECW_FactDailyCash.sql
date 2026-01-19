TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_FactDailyCash ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_FactDailyCash (DailyCashKey, DatePosted, SourceSystem, IccCoidFlag, PaymentType, Amount, PCNR2MoPrior, TotalBankDays, RunningTotalBankDays, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
SELECT source.DailyCashKey, source.DatePosted, TRIM(source.SourceSystem), TRIM(source.IccCoidFlag), TRIM(source.PaymentType), source.Amount, source.PCNR2MoPrior, source.TotalBankDays, source.RunningTotalBankDays, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime
FROM {{ params.param_psc_stage_dataset_name }}.ECW_FactDailyCash as source;