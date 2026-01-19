
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactInvoiceLinePayment AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactInvoiceLinePayment AS source
ON target.InvoiceLinePaymentKey = source.InvoiceLinePaymentKey
WHEN MATCHED THEN
  UPDATE SET
  target.InvoiceLinePaymentKey = source.InvoiceLinePaymentKey,
 target.Coid = TRIM(source.Coid),
 target.RegionKey = source.RegionKey,
 target.Practice = TRIM(source.Practice),
 target.InvoiceNumber = source.InvoiceNumber,
 target.InvoiceLineNumber = source.InvoiceLineNumber,
 target.InvoiceLineChargeKey = source.InvoiceLineChargeKey,
 target.PaymentAmt = source.PaymentAmt,
 target.PaymentDateKey = source.PaymentDateKey,
 target.PaymentType = TRIM(source.PaymentType),
 target.TransactionNumber = source.TransactionNumber,
 target.InvoiceKey = source.InvoiceKey,
 target.BatchKey = source.BatchKey,
 target.ClosingDateKey = source.ClosingDateKey,
 target.CreatedByUserKey = source.CreatedByUserKey,
 target.InvoiceTransactionDesc = TRIM(source.InvoiceTransactionDesc),
 target.InvoiceReason = TRIM(source.InvoiceReason),
 target.CheckNumber = TRIM(source.CheckNumber),
 target.CheckDateKey = source.CheckDateKey,
 target.CheckTypeDesc = TRIM(source.CheckTypeDesc),
 target.PaymentMsgcode = TRIM(source.PaymentMsgcode),
 target.DepositDateKey = source.DepositDateKey,
 target.PaymentNumber = source.PaymentNumber,
 target.TreasuryBatchNumber = TRIM(source.TreasuryBatchNumber),
 target.TreasuryBatchDepositDate = source.TreasuryBatchDepositDate,
 target.TreasuryBatchPayerName = TRIM(source.TreasuryBatchPayerName),
 target.DeleteFlag = source.DeleteFlag,
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBPrimaryKeyValue = TRIM(source.SourceBPrimaryKeyValue),
 target.SourceBRecordLastUpdated = source.SourceBRecordLastUpdated,
 target.DWLastUpdatedateTime = source.DWLastUpdatedateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (InvoiceLinePaymentKey, Coid, RegionKey, Practice, InvoiceNumber, InvoiceLineNumber, InvoiceLineChargeKey, PaymentAmt, PaymentDateKey, PaymentType, TransactionNumber, InvoiceKey, BatchKey, ClosingDateKey, CreatedByUserKey, InvoiceTransactionDesc, InvoiceReason, CheckNumber, CheckDateKey, CheckTypeDesc, PaymentMsgcode, DepositDateKey, PaymentNumber, TreasuryBatchNumber, TreasuryBatchDepositDate, TreasuryBatchPayerName, DeleteFlag, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBPrimaryKeyValue, SourceBRecordLastUpdated, DWLastUpdatedateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.InvoiceLinePaymentKey, TRIM(source.Coid), source.RegionKey, TRIM(source.Practice), source.InvoiceNumber, source.InvoiceLineNumber, source.InvoiceLineChargeKey, source.PaymentAmt, source.PaymentDateKey, TRIM(source.PaymentType), source.TransactionNumber, source.InvoiceKey, source.BatchKey, source.ClosingDateKey, source.CreatedByUserKey, TRIM(source.InvoiceTransactionDesc), TRIM(source.InvoiceReason), TRIM(source.CheckNumber), source.CheckDateKey, TRIM(source.CheckTypeDesc), TRIM(source.PaymentMsgcode), source.DepositDateKey, source.PaymentNumber, TRIM(source.TreasuryBatchNumber), source.TreasuryBatchDepositDate, TRIM(source.TreasuryBatchPayerName), source.DeleteFlag, TRIM(source.SourceAPrimaryKeyValue), source.SourceARecordLastUpdated, TRIM(source.SourceBPrimaryKeyValue), source.SourceBRecordLastUpdated, source.DWLastUpdatedateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT InvoiceLinePaymentKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactInvoiceLinePayment
      GROUP BY InvoiceLinePaymentKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactInvoiceLinePayment');
ELSE
  COMMIT TRANSACTION;
END IF;
