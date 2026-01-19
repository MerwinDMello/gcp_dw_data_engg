
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactClaimFinancialAdjustment AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactClaimFinancialAdjustment AS source
ON target.ClaimFinancialAdjustmentKey = source.ClaimFinancialAdjustmentKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimFinancialAdjustmentKey = source.ClaimFinancialAdjustmentKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.CoidConfigurationKey = source.CoidConfigurationKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ClaimPayer1IplanKey = source.ClaimPayer1IplanKey,
 target.FacilityKey = source.FacilityKey,
 target.AdjustmentCodeKey = source.AdjustmentCodeKey,
 target.AdjustmentAmt = source.AdjustmentAmt,
 target.AdjustmentDateKey = source.AdjustmentDateKey,
 target.UnpostedToCPTAmt = source.UnpostedToCPTAmt,
 target.UnpostedAmt = source.UnpostedAmt,
 target.ModifiedDateKey = source.ModifiedDateKey,
 target.ModifiedTime = source.ModifiedTime,
 target.CreatedByUserKey = source.CreatedByUserKey,
 target.ClosingDateKey = source.ClosingDateKey,
 target.DeleteFlag = source.DeleteFlag,
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBPrimaryKeyValue = source.SourceBPrimaryKeyValue,
 target.SourceBRecordLastUpdated = source.SourceBRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.AdjCode = TRIM(source.AdjCode),
 target.AdjustmentCategoryKey = source.AdjustmentCategoryKey,
 target.PracticeKey = source.PracticeKey,
 target.PracticeName = TRIM(source.PracticeName)
WHEN NOT MATCHED THEN
  INSERT (ClaimFinancialAdjustmentKey, ClaimKey, ClaimNumber, RegionKey, Coid, CoidConfigurationKey, ServicingProviderKey, ClaimPayer1IplanKey, FacilityKey, AdjustmentCodeKey, AdjustmentAmt, AdjustmentDateKey, UnpostedToCPTAmt, UnpostedAmt, ModifiedDateKey, ModifiedTime, CreatedByUserKey, ClosingDateKey, DeleteFlag, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBPrimaryKeyValue, SourceBRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, AdjCode, AdjustmentCategoryKey, PracticeKey, PracticeName)
  VALUES (source.ClaimFinancialAdjustmentKey, source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.Coid), source.CoidConfigurationKey, source.ServicingProviderKey, source.ClaimPayer1IplanKey, source.FacilityKey, source.AdjustmentCodeKey, source.AdjustmentAmt, source.AdjustmentDateKey, source.UnpostedToCPTAmt, source.UnpostedAmt, source.ModifiedDateKey, source.ModifiedTime, source.CreatedByUserKey, source.ClosingDateKey, source.DeleteFlag, TRIM(source.SourceAPrimaryKeyValue), source.SourceARecordLastUpdated, source.SourceBPrimaryKeyValue, source.SourceBRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.AdjCode), source.AdjustmentCategoryKey, source.PracticeKey, TRIM(source.PracticeName));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimFinancialAdjustmentKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactClaimFinancialAdjustment
      GROUP BY ClaimFinancialAdjustmentKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactClaimFinancialAdjustment');
ELSE
  COMMIT TRANSACTION;
END IF;
