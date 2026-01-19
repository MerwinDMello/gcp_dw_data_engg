
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterActionLog AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactEncounterActionLog AS source
ON target.EncounterActionLogID = source.EncounterActionLogID
WHEN MATCHED THEN
  UPDATE SET
  target.EncounterActionLogID = source.EncounterActionLogID,
 target.EncounterKey = source.EncounterKey,
 target.EncounterActionLogTypeKey = source.EncounterActionLogTypeKey,
 target.EncounterActionLogNote = TRIM(source.EncounterActionLogNote),
 target.EncounterActionLogDateKey = source.EncounterActionLogDateKey,
 target.EncounterActionLogTime = TRIM(source.EncounterActionLogTime),
 target.EncounterActionLogCreatedByUserKey = source.EncounterActionLogCreatedByUserKey,
 target.RegionKey = source.RegionKey,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.ArchivedRecord = TRIM(source.ArchivedRecord)
WHEN NOT MATCHED THEN
  INSERT (EncounterActionLogID, EncounterKey, EncounterActionLogTypeKey, EncounterActionLogNote, EncounterActionLogDateKey, EncounterActionLogTime, EncounterActionLogCreatedByUserKey, RegionKey, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ArchivedRecord)
  VALUES (source.EncounterActionLogID, source.EncounterKey, source.EncounterActionLogTypeKey, TRIM(source.EncounterActionLogNote), source.EncounterActionLogDateKey, TRIM(source.EncounterActionLogTime), source.EncounterActionLogCreatedByUserKey, source.RegionKey, source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.ArchivedRecord));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterActionLogID
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterActionLog
      GROUP BY EncounterActionLogID
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterActionLog');
ELSE
  COMMIT TRANSACTION;
END IF;
