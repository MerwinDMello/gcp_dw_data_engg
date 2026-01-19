
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactTransactionsClaimLineCharge AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactTransactionsClaimLineCharge AS source
ON target.TransactionsClaimLineChargeKey = source.TransactionsClaimLineChargeKey
WHEN MATCHED THEN
  UPDATE SET
  target.TransactionsClaimLineChargeKey = source.TransactionsClaimLineChargeKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.CoidConfigurationKey = source.CoidConfigurationKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ClaimPayer1IplanKey = source.ClaimPayer1IplanKey,
 target.FacilityKey = source.FacilityKey,
 target.ClaimLineChargeKey = source.ClaimLineChargeKey,
 target.CPTCodeKey = source.CPTCodeKey,
 target.TransactionType = TRIM(source.TransactionType),
 target.TransactionFlag = TRIM(source.TransactionFlag),
 target.TransactionAmt = source.TransactionAmt,
 target.TransactionDateKey = source.TransactionDateKey,
 target.TransactionTime = source.TransactionTime,
 target.TransactionClosingDateKey = source.TransactionClosingDateKey,
 target.TransactionByUserKey = source.TransactionByUserKey,
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.TrRefID = source.TrRefID,
 target.PracticeKey = source.PracticeKey,
 target.PracticeName = TRIM(source.PracticeName)
WHEN NOT MATCHED THEN
  INSERT (TransactionsClaimLineChargeKey, ClaimKey, ClaimNumber, RegionKey, Coid, CoidConfigurationKey, ServicingProviderKey, ClaimPayer1IplanKey, FacilityKey, ClaimLineChargeKey, CPTCodeKey, TransactionType, TransactionFlag, TransactionAmt, TransactionDateKey, TransactionTime, TransactionClosingDateKey, TransactionByUserKey, SourceAPrimaryKeyValue, SourceARecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, TrRefID, PracticeKey, PracticeName)
  VALUES (source.TransactionsClaimLineChargeKey, source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.Coid), source.CoidConfigurationKey, source.ServicingProviderKey, source.ClaimPayer1IplanKey, source.FacilityKey, source.ClaimLineChargeKey, source.CPTCodeKey, TRIM(source.TransactionType), TRIM(source.TransactionFlag), source.TransactionAmt, source.TransactionDateKey, source.TransactionTime, source.TransactionClosingDateKey, source.TransactionByUserKey, TRIM(source.SourceAPrimaryKeyValue), source.SourceARecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.TrRefID, source.PracticeKey, TRIM(source.PracticeName));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT TransactionsClaimLineChargeKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactTransactionsClaimLineCharge
      GROUP BY TransactionsClaimLineChargeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactTransactionsClaimLineCharge');
ELSE
  COMMIT TRANSACTION;
END IF;
