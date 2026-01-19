
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactTransactionsClaimLineContractualAdjustment AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactTransactionsClaimLineContractualAdjustment AS source
ON target.TransactionsClaimLineContractualAdjustmentsKey = source.TransactionsClaimLineContractualAdjustmentsKey
WHEN MATCHED THEN
  UPDATE SET
  target.TransactionsClaimLineContractualAdjustmentsKey = source.TransactionsClaimLineContractualAdjustmentsKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.CoidConfigurationKey = source.CoidConfigurationKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ClaimPayer1IplanKey = source.ClaimPayer1IplanKey,
 target.FacilityKey = source.FacilityKey,
 target.ClaimLinePaymentsKey = source.ClaimLinePaymentsKey,
 target.TransactionType = TRIM(source.TransactionType),
 target.TransactionFlag = TRIM(source.TransactionFlag),
 target.TransactionAmt = source.TransactionAmt,
 target.TransactionDateKey = source.TransactionDateKey,
 target.TransactionTime = source.TransactionTime,
 target.TransactionByUserKey = source.TransactionByUserKey,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.TrRefID = source.TrRefID,
 target.ArchivedRecord = TRIM(source.ArchivedRecord)
WHEN NOT MATCHED THEN
  INSERT (TransactionsClaimLineContractualAdjustmentsKey, ClaimKey, ClaimNumber, RegionKey, Coid, CoidConfigurationKey, ServicingProviderKey, ClaimPayer1IplanKey, FacilityKey, ClaimLinePaymentsKey, TransactionType, TransactionFlag, TransactionAmt, TransactionDateKey, TransactionTime, TransactionByUserKey, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, TrRefID, ArchivedRecord)
  VALUES (source.TransactionsClaimLineContractualAdjustmentsKey, source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.Coid), source.CoidConfigurationKey, source.ServicingProviderKey, source.ClaimPayer1IplanKey, source.FacilityKey, source.ClaimLinePaymentsKey, TRIM(source.TransactionType), TRIM(source.TransactionFlag), source.TransactionAmt, source.TransactionDateKey, source.TransactionTime, source.TransactionByUserKey, source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.TrRefID, TRIM(source.ArchivedRecord));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT TransactionsClaimLineContractualAdjustmentsKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactTransactionsClaimLineContractualAdjustment
      GROUP BY TransactionsClaimLineContractualAdjustmentsKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactTransactionsClaimLineContractualAdjustment');
ELSE
  COMMIT TRANSACTION;
END IF;
