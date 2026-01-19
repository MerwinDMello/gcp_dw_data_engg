
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefHoldCode AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefHoldCode AS source
ON target.HoldCodeKey = source.HoldCodeKey
WHEN MATCHED THEN
  UPDATE SET
  target.HoldCodeKey = source.HoldCodeKey,
 target.HoldCode = source.HoldCode,
 target.HoldCodeName = TRIM(source.HoldCodeName),
 target.HoldCodeType = TRIM(source.HoldCodeType),
 target.DeleteFlag = source.DeleteFlag,
 target.HoldCodeDescription = TRIM(source.HoldCodeDescription),
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (HoldCodeKey, HoldCode, HoldCodeName, HoldCodeType, DeleteFlag, HoldCodeDescription, RegionKey, SourceAPrimaryKeyValue, SourceARecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.HoldCodeKey, source.HoldCode, TRIM(source.HoldCodeName), TRIM(source.HoldCodeType), source.DeleteFlag, TRIM(source.HoldCodeDescription), source.RegionKey, TRIM(source.SourceAPrimaryKeyValue), source.SourceARecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT HoldCodeKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefHoldCode
      GROUP BY HoldCodeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefHoldCode');
ELSE
  COMMIT TRANSACTION;
END IF;
