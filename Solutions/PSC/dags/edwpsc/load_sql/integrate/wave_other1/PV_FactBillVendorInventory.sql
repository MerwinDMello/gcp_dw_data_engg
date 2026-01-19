
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactBillVendorInventory AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactBillVendorInventory AS source
ON target.BillVendorInventoryKey = source.BillVendorInventoryKey
WHEN MATCHED THEN
  UPDATE SET
  target.BillVendorInventoryKey = source.BillVendorInventoryKey,
 target.RegionKey = source.RegionKey,
 target.PracticeName = TRIM(source.PracticeName),
 target.Coid = TRIM(source.Coid),
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.ClaimDateKey = source.ClaimDateKey,
 target.TotalChargeAmt = source.TotalChargeAmt,
 target.ClaimStatus = source.ClaimStatus,
 target.ClaimStatusName = TRIM(source.ClaimStatusName),
 target.ClaimStatusLastModifiedDate = source.ClaimStatusLastModifiedDate,
 target.TranslatedDateTime = source.TranslatedDateTime,
 target.ClaimHistoryLastModifiedDate = source.ClaimHistoryLastModifiedDate,
 target.SSIBatchID = source.SSIBatchID,
 target.ClaimReleaseDate = source.ClaimReleaseDate,
 target.Category277 = TRIM(source.Category277),
 target.Status277 = TRIM(source.Status277),
 target.BilledStatus = TRIM(source.BilledStatus),
 target.HoldCode = TRIM(source.HoldCode),
 target.Destination = TRIM(source.Destination),
 target.BatchIdEcw = source.BatchIdEcw,
 target.FinancialGroup = TRIM(source.FinancialGroup),
 target.CurrentPayerName = TRIM(source.CurrentPayerName),
 target.CurrentPayorIndicator = source.CurrentPayorIndicator,
 target.Payer1IplanKey = source.Payer1IplanKey,
 target.Payer1IplanId = TRIM(source.Payer1IplanId),
 target.Payer1FinancialClass = TRIM(source.Payer1FinancialClass),
 target.Payer2IplanKey = source.Payer2IplanKey,
 target.Payer2IplanId = TRIM(source.Payer2IplanId),
 target.Payer2FinancialClass = TRIM(source.Payer2FinancialClass),
 target.Payer3IplanKey = source.Payer3IplanKey,
 target.Payer3IplanId = TRIM(source.Payer3IplanId),
 target.Payer3FinancialClass = TRIM(source.Payer3FinancialClass),
 target.FacilityKey = source.FacilityKey,
 target.FacilityID = source.FacilityID,
 target.FacilityCode = TRIM(source.FacilityCode),
 target.ProviderTaxId = TRIM(source.ProviderTaxId),
 target.OriginalClaimKey = source.OriginalClaimKey,
 target.OriginalClaimNumber = source.OriginalClaimNumber,
 target.DeleteFlag = source.DeleteFlag,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.TraceNumber = TRIM(source.TraceNumber),
 target.TrackStat = TRIM(source.TrackStat),
 target.BillDate = source.BillDate,
 target.PriLBillDate = source.PriLBillDate,
 target.SecBillDate = source.SecBillDate,
 target.SecLBillDate = source.SecLBillDate,
 target.TerBillDate = source.TerBillDate,
 target.TerLBillDate = source.TerLBillDate,
 target.ClaimType = TRIM(source.ClaimType),
 target.Dest = TRIM(source.Dest),
 target.SourceCode = TRIM(source.SourceCode),
 target.PayorID = TRIM(source.PayorID),
 target.PayorSubID = TRIM(source.PayorSubID),
 target.BillTrace = TRIM(source.BillTrace)
WHEN NOT MATCHED THEN
  INSERT (BillVendorInventoryKey, RegionKey, PracticeName, Coid, ClaimKey, ClaimNumber, ClaimDateKey, TotalChargeAmt, ClaimStatus, ClaimStatusName, ClaimStatusLastModifiedDate, TranslatedDateTime, ClaimHistoryLastModifiedDate, SSIBatchID, ClaimReleaseDate, Category277, Status277, BilledStatus, HoldCode, Destination, BatchIdEcw, FinancialGroup, CurrentPayerName, CurrentPayorIndicator, Payer1IplanKey, Payer1IplanId, Payer1FinancialClass, Payer2IplanKey, Payer2IplanId, Payer2FinancialClass, Payer3IplanKey, Payer3IplanId, Payer3FinancialClass, FacilityKey, FacilityID, FacilityCode, ProviderTaxId, OriginalClaimKey, OriginalClaimNumber, DeleteFlag, SourceAPrimaryKeyValue, SourceSystemCode, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, TraceNumber, TrackStat, BillDate, PriLBillDate, SecBillDate, SecLBillDate, TerBillDate, TerLBillDate, ClaimType, Dest, SourceCode, PayorID, PayorSubID, BillTrace)
  VALUES (source.BillVendorInventoryKey, source.RegionKey, TRIM(source.PracticeName), TRIM(source.Coid), source.ClaimKey, source.ClaimNumber, source.ClaimDateKey, source.TotalChargeAmt, source.ClaimStatus, TRIM(source.ClaimStatusName), source.ClaimStatusLastModifiedDate, source.TranslatedDateTime, source.ClaimHistoryLastModifiedDate, source.SSIBatchID, source.ClaimReleaseDate, TRIM(source.Category277), TRIM(source.Status277), TRIM(source.BilledStatus), TRIM(source.HoldCode), TRIM(source.Destination), source.BatchIdEcw, TRIM(source.FinancialGroup), TRIM(source.CurrentPayerName), source.CurrentPayorIndicator, source.Payer1IplanKey, TRIM(source.Payer1IplanId), TRIM(source.Payer1FinancialClass), source.Payer2IplanKey, TRIM(source.Payer2IplanId), TRIM(source.Payer2FinancialClass), source.Payer3IplanKey, TRIM(source.Payer3IplanId), TRIM(source.Payer3FinancialClass), source.FacilityKey, source.FacilityID, TRIM(source.FacilityCode), TRIM(source.ProviderTaxId), source.OriginalClaimKey, source.OriginalClaimNumber, source.DeleteFlag, source.SourceAPrimaryKeyValue, TRIM(source.SourceSystemCode), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.TraceNumber), TRIM(source.TrackStat), source.BillDate, source.PriLBillDate, source.SecBillDate, source.SecLBillDate, source.TerBillDate, source.TerLBillDate, TRIM(source.ClaimType), TRIM(source.Dest), TRIM(source.SourceCode), TRIM(source.PayorID), TRIM(source.PayorSubID), TRIM(source.BillTrace));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT BillVendorInventoryKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactBillVendorInventory
      GROUP BY BillVendorInventoryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactBillVendorInventory');
ELSE
  COMMIT TRANSACTION;
END IF;
