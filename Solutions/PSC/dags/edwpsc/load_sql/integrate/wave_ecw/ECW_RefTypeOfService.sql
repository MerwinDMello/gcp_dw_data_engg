
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefTypeOfService AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefTypeOfService AS source
ON target.TOSKey = source.TOSKey
WHEN MATCHED THEN
  UPDATE SET
  target.TOSKey = source.TOSKey,
 target.TOSCode = TRIM(source.TOSCode),
 target.TOSName = TRIM(source.TOSName),
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DeleteFlag = source.DeleteFlag,
 target.RegionKey = source.RegionKey
WHEN NOT MATCHED THEN
  INSERT (TOSKey, TOSCode, TOSName, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag, RegionKey)
  VALUES (source.TOSKey, TRIM(source.TOSCode), TRIM(source.TOSName), source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag, source.RegionKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT TOSKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefTypeOfService
      GROUP BY TOSKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefTypeOfService');
ELSE
  COMMIT TRANSACTION;
END IF;
