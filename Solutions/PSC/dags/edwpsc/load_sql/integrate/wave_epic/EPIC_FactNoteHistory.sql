
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactNoteHistory AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactNoteHistory AS source
ON target.NoteHistoryKey = source.NoteHistoryKey
WHEN MATCHED THEN
  UPDATE SET
  target.NoteHistoryKey = source.NoteHistoryKey,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.AccountKey = source.AccountKey,
 target.AccountId = source.AccountId,
 target.PatientKey = source.PatientKey,
 target.PatientId = source.PatientId,
 target.ClaimKey = source.ClaimKey,
 target.InvoiceNumber = TRIM(source.InvoiceNumber),
 target.EncounterKey = source.EncounterKey,
 target.EncounterId = source.EncounterId,
 target.NoteType = TRIM(source.NoteType),
 target.NoteSummary = TRIM(source.NoteSummary),
 target.Note = TRIM(source.Note),
 target.NoteStatus = TRIM(source.NoteStatus),
 target.NoteCreatedDate = source.NoteCreatedDate,
 target.NoteCreatedTime = source.NoteCreatedTime,
 target.PriorityFlag = source.PriorityFlag,
 target.NoteCreatedByUserKey = source.NoteCreatedByUserKey,
 target.NoteCreatedByUserID = TRIM(source.NoteCreatedByUserID),
 target.NoteSource = TRIM(source.NoteSource),
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (NoteHistoryKey, RegionKey, Coid, AccountKey, AccountId, PatientKey, PatientId, ClaimKey, InvoiceNumber, EncounterKey, EncounterId, NoteType, NoteSummary, Note, NoteStatus, NoteCreatedDate, NoteCreatedTime, PriorityFlag, NoteCreatedByUserKey, NoteCreatedByUserID, NoteSource, SourceAPrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.NoteHistoryKey, source.RegionKey, TRIM(source.Coid), source.AccountKey, source.AccountId, source.PatientKey, source.PatientId, source.ClaimKey, TRIM(source.InvoiceNumber), source.EncounterKey, source.EncounterId, TRIM(source.NoteType), TRIM(source.NoteSummary), TRIM(source.Note), TRIM(source.NoteStatus), source.NoteCreatedDate, source.NoteCreatedTime, source.PriorityFlag, source.NoteCreatedByUserKey, TRIM(source.NoteCreatedByUserID), TRIM(source.NoteSource), TRIM(source.SourceAPrimaryKeyValue), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT NoteHistoryKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactNoteHistory
      GROUP BY NoteHistoryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactNoteHistory');
ELSE
  COMMIT TRANSACTION;
END IF;
