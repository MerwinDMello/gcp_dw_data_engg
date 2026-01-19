
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PK_FactTransactionsHistoric AS target
USING {{ params.param_psc_stage_dataset_name }}.PK_FactTransactionsHistoric AS source
ON target.PKTransactionHistoricKey = source.PKTransactionHistoricKey
WHEN MATCHED THEN
  UPDATE SET
  target.PKTransactionHistoricKey = source.PKTransactionHistoricKey,
 target.PKRegionName = TRIM(source.PKRegionName),
 target.HistoricalStatus = TRIM(source.HistoricalStatus),
 target.SavedDate = source.SavedDate,
 target.SavedBy = TRIM(source.SavedBy),
 target.SavedByFirstName = TRIM(source.SavedByFirstName),
 target.SavedByLastName = TRIM(source.SavedByLastName),
 target.BillingArea = TRIM(source.BillingArea),
 target.BillingProvider = TRIM(source.BillingProvider),
 target.BillingProviderFirstName = TRIM(source.BillingProviderFirstName),
 target.BillingProviderLastName = TRIM(source.BillingProviderLastName),
 target.ServiceDate = source.ServiceDate,
 target.SubmissionDate = source.SubmissionDate,
 target.VisitType = TRIM(source.VisitType),
 target.Department = TRIM(source.Department),
 target.PatientMRN = TRIM(source.PatientMRN),
 target.ROLES = TRIM(source.ROLES),
 target.AccountId = source.AccountId,
 target.HistoricalFinancialClass = TRIM(source.HistoricalFinancialClass),
 target.CPTCode = TRIM(source.CPTCode),
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceBPrimaryKeyValue = source.SourceBPrimaryKeyValue,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.HeldForReview = source.HeldForReview,
 target.IsDeleted = source.IsDeleted,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (PKTransactionHistoricKey, PKRegionName, HistoricalStatus, SavedDate, SavedBy, SavedByFirstName, SavedByLastName, BillingArea, BillingProvider, BillingProviderFirstName, BillingProviderLastName, ServiceDate, SubmissionDate, VisitType, Department, PatientMRN, ROLES, AccountId, HistoricalFinancialClass, CPTCode, SourceAPrimaryKeyValue, SourceBPrimaryKeyValue, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, HeldForReview, IsDeleted, DWLastUpdateDateTime)
  VALUES (source.PKTransactionHistoricKey, TRIM(source.PKRegionName), TRIM(source.HistoricalStatus), source.SavedDate, TRIM(source.SavedBy), TRIM(source.SavedByFirstName), TRIM(source.SavedByLastName), TRIM(source.BillingArea), TRIM(source.BillingProvider), TRIM(source.BillingProviderFirstName), TRIM(source.BillingProviderLastName), source.ServiceDate, source.SubmissionDate, TRIM(source.VisitType), TRIM(source.Department), TRIM(source.PatientMRN), TRIM(source.ROLES), source.AccountId, TRIM(source.HistoricalFinancialClass), TRIM(source.CPTCode), source.SourceAPrimaryKeyValue, source.SourceBPrimaryKeyValue, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.HeldForReview, source.IsDeleted, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PKTransactionHistoricKey
      FROM {{ params.param_psc_core_dataset_name }}.PK_FactTransactionsHistoric
      GROUP BY PKTransactionHistoricKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PK_FactTransactionsHistoric');
ELSE
  COMMIT TRANSACTION;
END IF;
