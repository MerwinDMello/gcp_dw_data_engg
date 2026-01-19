
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_JuncPaymentMatchingTransactions AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_JuncPaymentMatchingTransactions AS source
ON target.TransactionId = source.TransactionId AND target.MatchingTransactionId = source.MatchingTransactionId AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.TransactionId = source.TransactionId,
 target.MatchingTransactionId = source.MatchingTransactionId,
 target.RegionKey = source.RegionKey,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (TransactionId, MatchingTransactionId, RegionKey, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.TransactionId, source.MatchingTransactionId, source.RegionKey, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT TransactionId, MatchingTransactionId, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_JuncPaymentMatchingTransactions
      GROUP BY TransactionId, MatchingTransactionId, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_JuncPaymentMatchingTransactions');
ELSE
  COMMIT TRANSACTION;
END IF;
