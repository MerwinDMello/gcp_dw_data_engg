
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactTeradataHPSTransactions AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactTeradataHPSTransactions AS source
ON target.TeradataHPSTransactionsKey = source.TeradataHPSTransactionsKey
WHEN MATCHED THEN
  UPDATE SET
  target.TeradataHPSTransactionsKey = source.TeradataHPSTransactionsKey,
 target.COID = TRIM(source.COID),
 target.RegionKey = source.RegionKey,
 target.LoadDateKey = source.LoadDateKey,
 target.HPSTransactionID = source.HPSTransactionID,
 target.TransactionTimestamp = source.TransactionTimestamp,
 target.TransactionDate = source.TransactionDate,
 target.EntryUserID = TRIM(source.EntryUserID),
 target.PaymentTenderType = TRIM(source.PaymentTenderType),
 target.PaymentReference = TRIM(source.PaymentReference),
 target.TransactionAmt = source.TransactionAmt,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (TeradataHPSTransactionsKey, COID, RegionKey, LoadDateKey, HPSTransactionID, TransactionTimestamp, TransactionDate, EntryUserID, PaymentTenderType, PaymentReference, TransactionAmt, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.TeradataHPSTransactionsKey, TRIM(source.COID), source.RegionKey, source.LoadDateKey, source.HPSTransactionID, source.TransactionTimestamp, source.TransactionDate, TRIM(source.EntryUserID), TRIM(source.PaymentTenderType), TRIM(source.PaymentReference), source.TransactionAmt, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT TeradataHPSTransactionsKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactTeradataHPSTransactions
      GROUP BY TeradataHPSTransactionsKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactTeradataHPSTransactions');
ELSE
  COMMIT TRANSACTION;
END IF;
