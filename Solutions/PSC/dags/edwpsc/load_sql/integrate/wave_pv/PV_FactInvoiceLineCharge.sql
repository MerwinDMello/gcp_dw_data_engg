
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactInvoiceLineCharge AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactInvoiceLineCharge AS source
ON target.InvoiceLineChargeKey = source.InvoiceLineChargeKey
WHEN MATCHED THEN
  UPDATE SET
  target.InvoiceLineChargeKey = source.InvoiceLineChargeKey,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.Practice = TRIM(source.Practice),
 target.InvoiceKey = source.InvoiceKey,
 target.InvoiceNumber = source.InvoiceNumber,
 target.InvoiceLineNumber = source.InvoiceLineNumber,
 target.ServiceDateKey = source.ServiceDateKey,
 target.InvoiceDescription = TRIM(source.InvoiceDescription),
 target.InvoiceUnits = source.InvoiceUnits,
 target.LineChargesPerUnit = source.LineChargesPerUnit,
 target.LineChargesAmt = source.LineChargesAmt,
 target.LinePaymentAmt = source.LinePaymentAmt,
 target.LineAdjustmentAmt = source.LineAdjustmentAmt,
 target.LineBalanceAmt = source.LineBalanceAmt,
 target.LineCalculatedBalance = source.LineCalculatedBalance,
 target.TransactionDateKey = source.TransactionDateKey,
 target.LastUpdatedByUserKey = source.LastUpdatedByUserKey,
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
  INSERT (InvoiceLineChargeKey, RegionKey, Coid, Practice, InvoiceKey, InvoiceNumber, InvoiceLineNumber, ServiceDateKey, InvoiceDescription, InvoiceUnits, LineChargesPerUnit, LineChargesAmt, LinePaymentAmt, LineAdjustmentAmt, LineBalanceAmt, LineCalculatedBalance, TransactionDateKey, LastUpdatedByUserKey, DeleteFlag, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBPrimaryKeyValue, SourceBRecordLastUpdated, DWLastUpdatedateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.InvoiceLineChargeKey, source.RegionKey, TRIM(source.Coid), TRIM(source.Practice), source.InvoiceKey, source.InvoiceNumber, source.InvoiceLineNumber, source.ServiceDateKey, TRIM(source.InvoiceDescription), source.InvoiceUnits, source.LineChargesPerUnit, source.LineChargesAmt, source.LinePaymentAmt, source.LineAdjustmentAmt, source.LineBalanceAmt, source.LineCalculatedBalance, source.TransactionDateKey, source.LastUpdatedByUserKey, source.DeleteFlag, TRIM(source.SourceAPrimaryKeyValue), source.SourceARecordLastUpdated, TRIM(source.SourceBPrimaryKeyValue), source.SourceBRecordLastUpdated, source.DWLastUpdatedateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT InvoiceLineChargeKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactInvoiceLineCharge
      GROUP BY InvoiceLineChargeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactInvoiceLineCharge');
ELSE
  COMMIT TRANSACTION;
END IF;
