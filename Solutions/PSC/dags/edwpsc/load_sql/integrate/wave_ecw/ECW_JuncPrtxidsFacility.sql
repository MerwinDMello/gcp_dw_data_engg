
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncPrtxidsFacility AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_JuncPrtxidsFacility AS source
ON target.ECWPrtxidsFacilityKey = source.ECWPrtxidsFacilityKey
WHEN MATCHED THEN
  UPDATE SET
  target.ECWPrtxidsFacilityKey = source.ECWPrtxidsFacilityKey,
 target.RegionKey = source.RegionKey,
 target.PrTxRuleId = source.PrTxRuleId,
 target.FacilityId = source.FacilityId,
 target.FacilityKey = source.FacilityKey,
 target.deleteFlag = source.deleteFlag,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ECWPrtxidsFacilityKey, RegionKey, PrTxRuleId, FacilityId, FacilityKey, deleteFlag, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ECWPrtxidsFacilityKey, source.RegionKey, source.PrTxRuleId, source.FacilityId, source.FacilityKey, source.deleteFlag, source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ECWPrtxidsFacilityKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_JuncPrtxidsFacility
      GROUP BY ECWPrtxidsFacilityKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_JuncPrtxidsFacility');
ELSE
  COMMIT TRANSACTION;
END IF;
