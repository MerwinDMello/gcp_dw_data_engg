
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactClaimLineFinancialAdjustment AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactClaimLineFinancialAdjustment AS source
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
 target.DeleteFlag = source.DeleteFlag,
 target.CreatedDateKey = source.CreatedDateKey,
 target.CreatedTime = source.CreatedTime,
 target.CreatedByUserKey = source.CreatedByUserKey,
 target.ModifiedDateKey = source.ModifiedDateKey,
 target.ModifiedTime = source.ModifiedTime,
 target.ModifiedByUserKey = source.ModifiedByUserKey,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.CPTID = source.CPTID,
 target.CPTCode = TRIM(source.CPTCode),
 target.CPTCodeKey = source.CPTCodeKey,
 target.CPTCharges = source.CPTCharges,
 target.CPTDeleteFlag = source.CPTDeleteFlag,
 target.AdjustmentCodeKey = source.AdjustmentCodeKey,
 target.ClaimAdjustmentAmt = source.ClaimAdjustmentAmt,
 target.ClaimAdjustmentDateKey = source.ClaimAdjustmentDateKey,
 target.ClaimAdjustmentDeleteFlag = source.ClaimAdjustmentDeleteFlag,
 target.AdjCode = TRIM(source.AdjCode),
 target.AdjustmentCategoryKey = source.AdjustmentCategoryKey,
 target.ArchivedRecord = TRIM(source.ArchivedRecord)
WHEN NOT MATCHED THEN
  INSERT (ClaimLineFinancialAdjustmentsKey, ClaimKey, ClaimNumber, RegionKey, Coid, CoidConfigurationKey, ServicingProviderKey, ClaimPayer1IplanKey, FacilityKey, ClaimAdjustmentKey, ClaimLineChargesKey, AdjustmentAmt, DeleteFlag, CreatedDateKey, CreatedTime, CreatedByUserKey, ModifiedDateKey, ModifiedTime, ModifiedByUserKey, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, CPTID, CPTCode, CPTCodeKey, CPTCharges, CPTDeleteFlag, AdjustmentCodeKey, ClaimAdjustmentAmt, ClaimAdjustmentDateKey, ClaimAdjustmentDeleteFlag, AdjCode, AdjustmentCategoryKey, ArchivedRecord)
  VALUES (source.ClaimLineFinancialAdjustmentsKey, source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.Coid), source.CoidConfigurationKey, source.ServicingProviderKey, source.ClaimPayer1IplanKey, source.FacilityKey, source.ClaimAdjustmentKey, source.ClaimLineChargesKey, source.AdjustmentAmt, source.DeleteFlag, source.CreatedDateKey, source.CreatedTime, source.CreatedByUserKey, source.ModifiedDateKey, source.ModifiedTime, source.ModifiedByUserKey, source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.CPTID, TRIM(source.CPTCode), source.CPTCodeKey, source.CPTCharges, source.CPTDeleteFlag, source.AdjustmentCodeKey, source.ClaimAdjustmentAmt, source.ClaimAdjustmentDateKey, source.ClaimAdjustmentDeleteFlag, TRIM(source.AdjCode), source.AdjustmentCategoryKey, TRIM(source.ArchivedRecord));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimLineFinancialAdjustmentsKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactClaimLineFinancialAdjustment
      GROUP BY ClaimLineFinancialAdjustmentsKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactClaimLineFinancialAdjustment');
ELSE
  COMMIT TRANSACTION;
END IF;
