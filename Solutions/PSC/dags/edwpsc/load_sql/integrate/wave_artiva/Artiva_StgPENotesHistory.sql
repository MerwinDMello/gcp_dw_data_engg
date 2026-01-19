
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPENotesHistory AS target
USING {{ params.param_psc_stage_dataset_name }}.Artiva_StgPENotesHistory AS source
ON target.PeNotesHistoryKey = source.PeNotesHistoryKey
WHEN MATCHED THEN
  UPDATE SET
  target.PeNotesHistoryKey = source.PeNotesHistoryKey,
 target.PSPEPPIKEY = TRIM(source.PSPEPPIKEY),
 target.NoteCount = source.NoteCount,
 target.NoteDate = source.NoteDate,
 target.NoteTime = source.NoteTime,
 target.NoteDateTime = source.NoteDateTime,
 target.NoteType = TRIM(source.NoteType),
 target.NoteCreatedByUserId = TRIM(source.NoteCreatedByUserId),
 target.Note = TRIM(source.Note),
 target.NoteSource = TRIM(source.NoteSource),
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDatetime = source.DWLastUpdateDatetime
WHEN NOT MATCHED THEN
  INSERT (PeNotesHistoryKey, PSPEPPIKEY, NoteCount, NoteDate, NoteTime, NoteDateTime, NoteType, NoteCreatedByUserId, Note, NoteSource, SourceAPrimaryKeyValue, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDatetime)
  VALUES (source.PeNotesHistoryKey, TRIM(source.PSPEPPIKEY), source.NoteCount, source.NoteDate, source.NoteTime, source.NoteDateTime, TRIM(source.NoteType), TRIM(source.NoteCreatedByUserId), TRIM(source.Note), TRIM(source.NoteSource), TRIM(source.SourceAPrimaryKeyValue), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDatetime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PeNotesHistoryKey
      FROM {{ params.param_psc_core_dataset_name }}.Artiva_StgPENotesHistory
      GROUP BY PeNotesHistoryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Artiva_StgPENotesHistory');
ELSE
  COMMIT TRANSACTION;
END IF;
