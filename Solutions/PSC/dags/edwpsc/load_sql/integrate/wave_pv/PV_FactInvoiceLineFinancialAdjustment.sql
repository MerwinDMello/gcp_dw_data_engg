
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactInvoiceLineFinancialAdjustment AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactInvoiceLineFinancialAdjustment AS source
ON target.InvoiceLineFinancialAdjustmentKey = source.InvoiceLineFinancialAdjustmentKey
WHEN MATCHED THEN
  UPDATE SET
  target.InvoiceLineFinancialAdjustmentKey = source.InvoiceLineFinancialAdjustmentKey,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.Practice = TRIM(source.Practice),
 target.InvoiceKey = source.InvoiceKey,
 target.InvoiceNumber = source.InvoiceNumber,
 target.InvoiceLineNumber = source.InvoiceLineNumber,
 target.InvoiceLineChargeKey = source.InvoiceLineChargeKey,
 target.AdjustmentAmt = source.AdjustmentAmt,
 target.AdjustmentDateKey = source.AdjustmentDateKey,
 target.AdjustmentCode = TRIM(source.AdjustmentCode),
 target.AdjustmentCodeKey = source.AdjustmentCodeKey,
 target.CreatedByUserKey = source.CreatedByUserKey,
 target.AdjustmentDesc = TRIM(source.AdjustmentDesc),
 target.TransactionNumber = source.TransactionNumber,
 target.BatchKey = source.BatchKey,
 target.CheckNumber = TRIM(source.CheckNumber),
 target.CheckDateKey = source.CheckDateKey,
 target.CheckTypeDesc = TRIM(source.CheckTypeDesc),
 target.PaymentMsgcode = TRIM(source.PaymentMsgcode),
 target.DepositDateKey = source.DepositDateKey,
 target.PaymentNumber = source.PaymentNumber,
 target.TreasuryBatchNumber = TRIM(source.TreasuryBatchNumber),
 target.TreasuryBatchDepositDate = source.TreasuryBatchDepositDate,
 target.TreasuryBatchPayerName = TRIM(source.TreasuryBatchPayerName),
 target.ClosingDateKey = source.ClosingDateKey,
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
  INSERT (InvoiceLineFinancialAdjustmentKey, RegionKey, Coid, Practice, InvoiceKey, InvoiceNumber, InvoiceLineNumber, InvoiceLineChargeKey, AdjustmentAmt, AdjustmentDateKey, AdjustmentCode, AdjustmentCodeKey, CreatedByUserKey, AdjustmentDesc, TransactionNumber, BatchKey, CheckNumber, CheckDateKey, CheckTypeDesc, PaymentMsgcode, DepositDateKey, PaymentNumber, TreasuryBatchNumber, TreasuryBatchDepositDate, TreasuryBatchPayerName, ClosingDateKey, DeleteFlag, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBPrimaryKeyValue, SourceBRecordLastUpdated, DWLastUpdatedateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.InvoiceLineFinancialAdjustmentKey, source.RegionKey, TRIM(source.Coid), TRIM(source.Practice), source.InvoiceKey, source.InvoiceNumber, source.InvoiceLineNumber, source.InvoiceLineChargeKey, source.AdjustmentAmt, source.AdjustmentDateKey, TRIM(source.AdjustmentCode), source.AdjustmentCodeKey, source.CreatedByUserKey, TRIM(source.AdjustmentDesc), source.TransactionNumber, source.BatchKey, TRIM(source.CheckNumber), source.CheckDateKey, TRIM(source.CheckTypeDesc), TRIM(source.PaymentMsgcode), source.DepositDateKey, source.PaymentNumber, TRIM(source.TreasuryBatchNumber), source.TreasuryBatchDepositDate, TRIM(source.TreasuryBatchPayerName), source.ClosingDateKey, source.DeleteFlag, TRIM(source.SourceAPrimaryKeyValue), source.SourceARecordLastUpdated, TRIM(source.SourceBPrimaryKeyValue), source.SourceBRecordLastUpdated, source.DWLastUpdatedateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT InvoiceLineFinancialAdjustmentKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactInvoiceLineFinancialAdjustment
      GROUP BY InvoiceLineFinancialAdjustmentKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactInvoiceLineFinancialAdjustment');
ELSE
  COMMIT TRANSACTION;
END IF;
