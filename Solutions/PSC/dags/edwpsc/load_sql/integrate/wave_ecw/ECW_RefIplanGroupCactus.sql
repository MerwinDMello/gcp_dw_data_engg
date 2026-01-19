
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefIplanGroupCactus AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefIplanGroupCactus AS source
ON target.IplanGroupCactusKey = source.IplanGroupCactusKey
WHEN MATCHED THEN
  UPDATE SET
  target.IplanGroupCactusKey = source.IplanGroupCactusKey,
 target.IplanGroupCactusName = TRIM(source.IplanGroupCactusName),
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DeleteFlag = source.DeleteFlag
WHEN NOT MATCHED THEN
  INSERT (IplanGroupCactusKey, IplanGroupCactusName, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag)
  VALUES (source.IplanGroupCactusKey, TRIM(source.IplanGroupCactusName), source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT IplanGroupCactusKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefIplanGroupCactus
      GROUP BY IplanGroupCactusKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefIplanGroupCactus');
ELSE
  COMMIT TRANSACTION;
END IF;
