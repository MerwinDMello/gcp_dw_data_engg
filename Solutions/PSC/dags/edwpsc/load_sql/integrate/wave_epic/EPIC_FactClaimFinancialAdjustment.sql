
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimFinancialAdjustment AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactClaimFinancialAdjustment AS source
ON target.ClaimFinancialAdjustmentKey = source.ClaimFinancialAdjustmentKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimFinancialAdjustmentKey = source.ClaimFinancialAdjustmentKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.TransactionNumber = source.TransactionNumber,
 target.VisitNumber = source.VisitNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.CoidConfigurationKey = source.CoidConfigurationKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ClaimPayer1IplanKey = source.ClaimPayer1IplanKey,
 target.ClaimLineLiabilityIplanKey = source.ClaimLineLiabilityIplanKey,
 target.FacilityKey = source.FacilityKey,
 target.AdjustmentCodeKey = source.AdjustmentCodeKey,
 target.AdjCode = TRIM(source.AdjCode),
 target.AdjustmentCategoryKey = source.AdjustmentCategoryKey,
 target.AdjustmentAmt = source.AdjustmentAmt,
 target.AdjustmentDateKey = source.AdjustmentDateKey,
 target.UnpostedToCPTAmt = source.UnpostedToCPTAmt,
 target.UnpostedAmt = source.UnpostedAmt,
 target.ModifiedDateKey = source.ModifiedDateKey,
 target.ModifiedTime = source.ModifiedTime,
 target.CreatedByUserKey = source.CreatedByUserKey,
 target.CreatedDateKey = source.CreatedDateKey,
 target.DeleteFlag = source.DeleteFlag,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBPrimaryKeyValue = TRIM(source.SourceBPrimaryKeyValue),
 target.SourceBRecordLastUpdated = source.SourceBRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.IplanKey = source.IplanKey
WHEN NOT MATCHED THEN
  INSERT (ClaimFinancialAdjustmentKey, ClaimKey, ClaimNumber, TransactionNumber, VisitNumber, RegionKey, Coid, CoidConfigurationKey, ServicingProviderKey, ClaimPayer1IplanKey, ClaimLineLiabilityIplanKey, FacilityKey, AdjustmentCodeKey, AdjCode, AdjustmentCategoryKey, AdjustmentAmt, AdjustmentDateKey, UnpostedToCPTAmt, UnpostedAmt, ModifiedDateKey, ModifiedTime, CreatedByUserKey, CreatedDateKey, DeleteFlag, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBPrimaryKeyValue, SourceBRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, IplanKey)
  VALUES (source.ClaimFinancialAdjustmentKey, source.ClaimKey, source.ClaimNumber, source.TransactionNumber, source.VisitNumber, source.RegionKey, TRIM(source.Coid), source.CoidConfigurationKey, source.ServicingProviderKey, source.ClaimPayer1IplanKey, source.ClaimLineLiabilityIplanKey, source.FacilityKey, source.AdjustmentCodeKey, TRIM(source.AdjCode), source.AdjustmentCategoryKey, source.AdjustmentAmt, source.AdjustmentDateKey, source.UnpostedToCPTAmt, source.UnpostedAmt, source.ModifiedDateKey, source.ModifiedTime, source.CreatedByUserKey, source.CreatedDateKey, source.DeleteFlag, source.SourceAPrimaryKeyValue, source.SourceARecordLastUpdated, TRIM(source.SourceBPrimaryKeyValue), source.SourceBRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.IplanKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimFinancialAdjustmentKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimFinancialAdjustment
      GROUP BY ClaimFinancialAdjustmentKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimFinancialAdjustment');
ELSE
  COMMIT TRANSACTION;
END IF;
