
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactTransactionsClaimFinancialAdjustment AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactTransactionsClaimFinancialAdjustment AS source
ON target.TransactionsClaimFinancialAdjustmentKey = source.TransactionsClaimFinancialAdjustmentKey
WHEN MATCHED THEN
  UPDATE SET
  target.TransactionsClaimFinancialAdjustmentKey = source.TransactionsClaimFinancialAdjustmentKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.TransactionNumber = source.TransactionNumber,
 target.VisitNumber = source.VisitNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.CoidConfigurationKey = source.CoidConfigurationKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ClaimPayer1IplanKey = source.ClaimPayer1IplanKey,
 target.ClaimLineLiabilityIplanKey = source.ClaimLineLiabilityIplanKey,
 target.FacilityKey = source.FacilityKey,
 target.ClaimFinancialAdjustmentKey = source.ClaimFinancialAdjustmentKey,
 target.AdjustmentCodeKey = source.AdjustmentCodeKey,
 target.TrRefID = source.TrRefID,
 target.TransactionType = TRIM(source.TransactionType),
 target.TransactionFlag = TRIM(source.TransactionFlag),
 target.TransactionAmt = source.TransactionAmt,
 target.TransactionDateKey = source.TransactionDateKey,
 target.TransactionTime = source.TransactionTime,
 target.TransactionByUserKey = source.TransactionByUserKey,
 target.TransactionPeriod = TRIM(source.TransactionPeriod),
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBPrimaryKeyValue = TRIM(source.SourceBPrimaryKeyValue),
 target.SourceBRecordLastUpdated = source.SourceBRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.MatchingTransactionNumber = source.MatchingTransactionNumber,
 target.DebitCreditFlag = source.DebitCreditFlag,
 target.MatchingTrRefId = source.MatchingTrRefId,
 target.AccountKey = source.AccountKey,
 target.PatientKey = source.PatientKey,
 target.DistributedByUserKey = TRIM(source.DistributedByUserKey),
 target.UndistributedByUserKey = TRIM(source.UndistributedByUserKey),
 target.HistMatchBatch = TRIM(source.HistMatchBatch),
 target.HistMatchUnbatch = TRIM(source.HistMatchUnbatch),
 target.EtrId = source.EtrId,
 target.MatchingEtrId = source.MatchingEtrId
WHEN NOT MATCHED THEN
  INSERT (TransactionsClaimFinancialAdjustmentKey, ClaimKey, ClaimNumber, TransactionNumber, VisitNumber, RegionKey, Coid, CoidConfigurationKey, ServicingProviderKey, ClaimPayer1IplanKey, ClaimLineLiabilityIplanKey, FacilityKey, ClaimFinancialAdjustmentKey, AdjustmentCodeKey, TrRefID, TransactionType, TransactionFlag, TransactionAmt, TransactionDateKey, TransactionTime, TransactionByUserKey, TransactionPeriod, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBPrimaryKeyValue, SourceBRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, MatchingTransactionNumber, DebitCreditFlag, MatchingTrRefId, AccountKey, PatientKey, DistributedByUserKey, UndistributedByUserKey, HistMatchBatch, HistMatchUnbatch, EtrId, MatchingEtrId)
  VALUES (source.TransactionsClaimFinancialAdjustmentKey, source.ClaimKey, source.ClaimNumber, source.TransactionNumber, source.VisitNumber, source.RegionKey, TRIM(source.Coid), source.CoidConfigurationKey, source.ServicingProviderKey, source.ClaimPayer1IplanKey, source.ClaimLineLiabilityIplanKey, source.FacilityKey, source.ClaimFinancialAdjustmentKey, source.AdjustmentCodeKey, source.TrRefID, TRIM(source.TransactionType), TRIM(source.TransactionFlag), source.TransactionAmt, source.TransactionDateKey, source.TransactionTime, source.TransactionByUserKey, TRIM(source.TransactionPeriod), source.SourceAPrimaryKeyValue, source.SourceARecordLastUpdated, TRIM(source.SourceBPrimaryKeyValue), source.SourceBRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.MatchingTransactionNumber, source.DebitCreditFlag, source.MatchingTrRefId, source.AccountKey, source.PatientKey, TRIM(source.DistributedByUserKey), TRIM(source.UndistributedByUserKey), TRIM(source.HistMatchBatch), TRIM(source.HistMatchUnbatch), source.EtrId, source.MatchingEtrId);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT TransactionsClaimFinancialAdjustmentKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactTransactionsClaimFinancialAdjustment
      GROUP BY TransactionsClaimFinancialAdjustmentKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactTransactionsClaimFinancialAdjustment');
ELSE
  COMMIT TRANSACTION;
END IF;
