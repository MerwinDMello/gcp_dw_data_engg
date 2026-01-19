
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotInvoice AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactSnapShotInvoice AS source
ON target.SnapShotDate = source.SnapShotDate AND target.InvoiceSnapShotKey = source.InvoiceSnapShotKey
WHEN MATCHED THEN
  UPDATE SET
  target.InvoiceSnapShotKey = source.InvoiceSnapShotKey,
 target.MonthId = source.MonthId,
 target.SnapShotDate = source.SnapShotDate,
 target.InvoiceKey = source.InvoiceKey,
 target.Coid = TRIM(source.Coid),
 target.RegionKey = source.RegionKey,
 target.Practice = TRIM(source.Practice),
 target.InvoiceNumber = source.InvoiceNumber,
 target.CompanyNumber = source.CompanyNumber,
 target.InvoiceDate = source.InvoiceDate,
 target.InvoiceType = TRIM(source.InvoiceType),
 target.TotalChargesAmt = source.TotalChargesAmt,
 target.TotalPaymentAmt = source.TotalPaymentAmt,
 target.TotalAdjustmentAmt = source.TotalAdjustmentAmt,
 target.TotalBalanceAmt = source.TotalBalanceAmt,
 target.TotalEndingBalanceAmt = source.TotalEndingBalanceAmt,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ServicingProviderID = source.ServicingProviderID,
 target.ReferringProviderKey = source.ReferringProviderKey,
 target.ReferringProviderID = source.ReferringProviderID,
 target.CompanyIplanKey = source.CompanyIplanKey,
 target.CompanyIplanID = TRIM(source.CompanyIplanID),
 target.LastUpdatedByUserKey = source.LastUpdatedByUserKey,
 target.LastUpdatedByUserID = TRIM(source.LastUpdatedByUserID),
 target.ClosingDateKey = source.ClosingDateKey,
 target.InternalNotes = TRIM(source.InternalNotes),
 target.ExternalNotes = TRIM(source.ExternalNotes),
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
  INSERT (InvoiceSnapShotKey, MonthId, SnapShotDate, InvoiceKey, Coid, RegionKey, Practice, InvoiceNumber, CompanyNumber, InvoiceDate, InvoiceType, TotalChargesAmt, TotalPaymentAmt, TotalAdjustmentAmt, TotalBalanceAmt, TotalEndingBalanceAmt, ServicingProviderKey, ServicingProviderID, ReferringProviderKey, ReferringProviderID, CompanyIplanKey, CompanyIplanID, LastUpdatedByUserKey, LastUpdatedByUserID, ClosingDateKey, InternalNotes, ExternalNotes, DeleteFlag, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBPrimaryKeyValue, SourceBRecordLastUpdated, DWLastUpdatedateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.InvoiceSnapShotKey, source.MonthId, source.SnapShotDate, source.InvoiceKey, TRIM(source.Coid), source.RegionKey, TRIM(source.Practice), source.InvoiceNumber, source.CompanyNumber, source.InvoiceDate, TRIM(source.InvoiceType), source.TotalChargesAmt, source.TotalPaymentAmt, source.TotalAdjustmentAmt, source.TotalBalanceAmt, source.TotalEndingBalanceAmt, source.ServicingProviderKey, source.ServicingProviderID, source.ReferringProviderKey, source.ReferringProviderID, source.CompanyIplanKey, TRIM(source.CompanyIplanID), source.LastUpdatedByUserKey, TRIM(source.LastUpdatedByUserID), source.ClosingDateKey, TRIM(source.InternalNotes), TRIM(source.ExternalNotes), source.DeleteFlag, TRIM(source.SourceAPrimaryKeyValue), source.SourceARecordLastUpdated, TRIM(source.SourceBPrimaryKeyValue), source.SourceBRecordLastUpdated, source.DWLastUpdatedateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, InvoiceSnapShotKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotInvoice
      GROUP BY SnapShotDate, InvoiceSnapShotKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotInvoice');
ELSE
  COMMIT TRANSACTION;
END IF;
