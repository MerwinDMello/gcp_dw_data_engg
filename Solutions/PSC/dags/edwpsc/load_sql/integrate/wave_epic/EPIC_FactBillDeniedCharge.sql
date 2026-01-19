
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactBillDeniedCharge AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactBillDeniedCharge AS source
ON target.BillDeniedChargeKey = source.BillDeniedChargeKey
WHEN MATCHED THEN
  UPDATE SET
  target.SourcePaymentTransactionID = TRIM(source.SourcePaymentTransactionID),
 target.BillDeniedChargeReceivedDate = source.BillDeniedChargeReceivedDate,
 target.BillDeniedChargeAmount = source.BillDeniedChargeAmount,
 target.BillDeniedChargeID = source.BillDeniedChargeID,
 target.BillDeniedChargeName = TRIM(source.BillDeniedChargeName),
 target.InvoiceNumber = TRIM(source.InvoiceNumber),
 target.DenialCode = TRIM(source.DenialCode),
 target.DenialCodeDesc = TRIM(source.DenialCodeDesc),
 target.DenialCodeRanking = source.DenialCodeRanking,
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.billdeniedchargekey = source.billdeniedchargekey
WHEN NOT MATCHED THEN
  INSERT (SourcePaymentTransactionID, BillDeniedChargeReceivedDate, BillDeniedChargeAmount, BillDeniedChargeID, BillDeniedChargeName, InvoiceNumber, DenialCode, DenialCodeDesc, DenialCodeRanking, RegionKey, SourceAPrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM,billdeniedchargekey)
  VALUES (TRIM(source.SourcePaymentTransactionID), source.BillDeniedChargeReceivedDate, source.BillDeniedChargeAmount, source.BillDeniedChargeID, TRIM(source.BillDeniedChargeName), TRIM(source.InvoiceNumber), TRIM(source.DenialCode), TRIM(source.DenialCodeDesc), source.DenialCodeRanking, source.RegionKey, source.SourceAPrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM,source.billdeniedchargekey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT BillDeniedChargeKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactBillDeniedCharge
      GROUP BY BillDeniedChargeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactBillDeniedCharge');
ELSE
  COMMIT TRANSACTION;
END IF;
