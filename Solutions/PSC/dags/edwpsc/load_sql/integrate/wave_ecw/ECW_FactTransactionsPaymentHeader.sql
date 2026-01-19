
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactTransactionsPaymentHeader AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactTransactionsPaymentHeader AS source
ON target.TransactionPaymentHeaderKey = source.TransactionPaymentHeaderKey
WHEN MATCHED THEN
  UPDATE SET
  target.TransactionPaymentHeaderKey = source.TransactionPaymentHeaderKey,
 target.RegionKey = source.RegionKey,
 target.PaymentHeaderDate = source.PaymentHeaderDate,
 target.PaymentHeaderTime = source.PaymentHeaderTime,
 target.PaymentHeaderUserKey = source.PaymentHeaderUserKey,
 target.PaymentHeaderUserId = source.PaymentHeaderUserId,
 target.TransactionFlag = TRIM(source.TransactionFlag),
 target.PaymentId = source.PaymentId,
 target.PaymentHeaderAmt = source.PaymentHeaderAmt,
 target.PaymentHeaderDescription = TRIM(source.PaymentHeaderDescription),
 target.PaymentHeaderModifiedDate = source.PaymentHeaderModifiedDate,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (TransactionPaymentHeaderKey, RegionKey, PaymentHeaderDate, PaymentHeaderTime, PaymentHeaderUserKey, PaymentHeaderUserId, TransactionFlag, PaymentId, PaymentHeaderAmt, PaymentHeaderDescription, PaymentHeaderModifiedDate, SourceAPrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.TransactionPaymentHeaderKey, source.RegionKey, source.PaymentHeaderDate, source.PaymentHeaderTime, source.PaymentHeaderUserKey, source.PaymentHeaderUserId, TRIM(source.TransactionFlag), source.PaymentId, source.PaymentHeaderAmt, TRIM(source.PaymentHeaderDescription), source.PaymentHeaderModifiedDate, source.SourceAPrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT TransactionPaymentHeaderKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactTransactionsPaymentHeader
      GROUP BY TransactionPaymentHeaderKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactTransactionsPaymentHeader');
ELSE
  COMMIT TRANSACTION;
END IF;
