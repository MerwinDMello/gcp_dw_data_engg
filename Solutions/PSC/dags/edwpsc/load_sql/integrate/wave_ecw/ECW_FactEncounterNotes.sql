
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterNotes AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactEncounterNotes AS source
ON target.EncounterNotesKey = source.EncounterNotesKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncounterNotesKey = source.EncounterNotesKey,
 target.RegionKey = source.RegionKey,
 target.EncounterKey = source.EncounterKey,
 target.EncounterId = source.EncounterId,
 target.EncounterNoteType = TRIM(source.EncounterNoteType),
 target.EncounterNote = TRIM(source.EncounterNote),
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.ArchivedRecord = TRIM(source.ArchivedRecord)
WHEN NOT MATCHED THEN
  INSERT (EncounterNotesKey, RegionKey, EncounterKey, EncounterId, EncounterNoteType, EncounterNote, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ArchivedRecord)
  VALUES (source.EncounterNotesKey, source.RegionKey, source.EncounterKey, source.EncounterId, TRIM(source.EncounterNoteType), TRIM(source.EncounterNote), source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.ArchivedRecord));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterNotesKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterNotes
      GROUP BY EncounterNotesKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterNotes');
ELSE
  COMMIT TRANSACTION;
END IF;
