
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactClaimLineFinancialAdjustment AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactClaimLineFinancialAdjustment AS source
ON target.ClaimLineFinancialAdjustmentsKey = source.ClaimLineFinancialAdjustmentsKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimLineFinancialAdjustmentsKey = source.ClaimLineFinancialAdjustmentsKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.CoidConfigurationKey = source.CoidConfigurationKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ClaimPayer1IplanKey = source.ClaimPayer1IplanKey,
 target.FacilityKey = source.FacilityKey,
 target.ClaimAdjustmentKey = source.ClaimAdjustmentKey,
 target.ClaimLineChargesKey = source.ClaimLineChargesKey,
 target.AdjustmentAmt = source.AdjustmentAmt,
 target.AdjustmentCodeKey = source.AdjustmentCodeKey,
 target.AdjCode = TRIM(source.AdjCode),
 target.AdjustmentCategoryKey = source.AdjustmentCategoryKey,
 target.CPTID = TRIM(source.CPTID),
 target.CPTCode = TRIM(source.CPTCode),
 target.CPTCodeKey = source.CPTCodeKey,
 target.CPTCharges = source.CPTCharges,
 target.CPTDeleteFlag = source.CPTDeleteFlag,
 target.ClaimAdjustmentAmt = source.ClaimAdjustmentAmt,
 target.ClaimAdjustmentDateKey = source.ClaimAdjustmentDateKey,
 target.ClaimAdjustmentDeleteFlag = source.ClaimAdjustmentDeleteFlag,
 target.DeleteFlag = source.DeleteFlag,
 target.CreatedDateKey = source.CreatedDateKey,
 target.CreatedTime = source.CreatedTime,
 target.CreatedByUserKey = source.CreatedByUserKey,
 target.ClosingDateKey = source.ClosingDateKey,
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBPrimaryKeyValue = TRIM(source.SourceBPrimaryKeyValue),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ClaimLineFinancialAdjustmentsKey, ClaimKey, ClaimNumber, RegionKey, Coid, CoidConfigurationKey, ServicingProviderKey, ClaimPayer1IplanKey, FacilityKey, ClaimAdjustmentKey, ClaimLineChargesKey, AdjustmentAmt, AdjustmentCodeKey, AdjCode, AdjustmentCategoryKey, CPTID, CPTCode, CPTCodeKey, CPTCharges, CPTDeleteFlag, ClaimAdjustmentAmt, ClaimAdjustmentDateKey, ClaimAdjustmentDeleteFlag, DeleteFlag, CreatedDateKey, CreatedTime, CreatedByUserKey, ClosingDateKey, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBPrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ClaimLineFinancialAdjustmentsKey, source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.Coid), source.CoidConfigurationKey, source.ServicingProviderKey, source.ClaimPayer1IplanKey, source.FacilityKey, source.ClaimAdjustmentKey, source.ClaimLineChargesKey, source.AdjustmentAmt, source.AdjustmentCodeKey, TRIM(source.AdjCode), source.AdjustmentCategoryKey, TRIM(source.CPTID), TRIM(source.CPTCode), source.CPTCodeKey, source.CPTCharges, source.CPTDeleteFlag, source.ClaimAdjustmentAmt, source.ClaimAdjustmentDateKey, source.ClaimAdjustmentDeleteFlag, source.DeleteFlag, source.CreatedDateKey, source.CreatedTime, source.CreatedByUserKey, source.ClosingDateKey, TRIM(source.SourceAPrimaryKeyValue), source.SourceARecordLastUpdated, TRIM(source.SourceBPrimaryKeyValue), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimLineFinancialAdjustmentsKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactClaimLineFinancialAdjustment
      GROUP BY ClaimLineFinancialAdjustmentsKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactClaimLineFinancialAdjustment');
ELSE
  COMMIT TRANSACTION;
END IF;
