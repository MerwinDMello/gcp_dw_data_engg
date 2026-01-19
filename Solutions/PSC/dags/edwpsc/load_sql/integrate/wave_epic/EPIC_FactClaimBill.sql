
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimBill AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactClaimBill AS source
ON target.ClaimBillKey = source.ClaimBillKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimBillKey = source.ClaimBillKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.VisitNumber = source.VisitNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.CoidConfigurationKey = source.CoidConfigurationKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ClaimPayer1IplanKey = source.ClaimPayer1IplanKey,
 target.FacilityKey = source.FacilityKey,
 target.InvoiceNumber = TRIM(source.InvoiceNumber),
 target.InvoiceStatus = TRIM(source.InvoiceStatus),
 target.BillTypeKey = source.BillTypeKey,
 target.IplanKey = source.IplanKey,
 target.BillMessage = TRIM(source.BillMessage),
 target.BatchNumber = TRIM(source.BatchNumber),
 target.BillDateKey = source.BillDateKey,
 target.BillTime = source.BillTime,
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBPrimaryKeyValue = TRIM(source.SourceBPrimaryKeyValue),
 target.SourceBRecordLastUpdated = source.SourceBRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ClaimBillKey, ClaimKey, ClaimNumber, VisitNumber, RegionKey, Coid, CoidConfigurationKey, ServicingProviderKey, ClaimPayer1IplanKey, FacilityKey, InvoiceNumber, InvoiceStatus, BillTypeKey, IplanKey, BillMessage, BatchNumber, BillDateKey, BillTime, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBPrimaryKeyValue, SourceBRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ClaimBillKey, source.ClaimKey, source.ClaimNumber, source.VisitNumber, source.RegionKey, TRIM(source.Coid), source.CoidConfigurationKey, source.ServicingProviderKey, source.ClaimPayer1IplanKey, source.FacilityKey, TRIM(source.InvoiceNumber), TRIM(source.InvoiceStatus), source.BillTypeKey, source.IplanKey, TRIM(source.BillMessage), TRIM(source.BatchNumber), source.BillDateKey, source.BillTime, TRIM(source.SourceAPrimaryKeyValue), source.SourceARecordLastUpdated, TRIM(source.SourceBPrimaryKeyValue), source.SourceBRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimBillKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimBill
      GROUP BY ClaimBillKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimBill');
ELSE
  COMMIT TRANSACTION;
END IF;
