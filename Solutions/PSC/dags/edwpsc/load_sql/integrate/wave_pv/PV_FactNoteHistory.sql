
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactNoteHistory AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactNoteHistory AS source
ON target.NoteHistoryKey = source.NoteHistoryKey
WHEN MATCHED THEN
  UPDATE SET
  target.NoteHistoryKey = source.NoteHistoryKey,
 target.RegionKey = source.RegionKey,
 target.PracticeKey = source.PracticeKey,
 target.Coid = TRIM(source.Coid),
 target.PatientKey = source.PatientKey,
 target.Note = TRIM(source.Note),
 target.NoteType = TRIM(source.NoteType),
 target.NoteCreatedDate = source.NoteCreatedDate,
 target.NoteCreatedTime = source.NoteCreatedTime,
 target.NoteCreatedByUserKey = source.NoteCreatedByUserKey,
 target.NoteCreatedByUserID = TRIM(source.NoteCreatedByUserID),
 target.NoteLastUpdatedByUserKey = source.NoteLastUpdatedByUserKey,
 target.NoteLastUpdatedByUserID = TRIM(source.NoteLastUpdatedByUserID),
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.NotesPK = TRIM(source.NotesPK),
 target.PatientSourceGuid = TRIM(source.PatientSourceGuid),
 target.ActiveFlag = source.ActiveFlag,
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.CoderNoteFlag = source.CoderNoteFlag
WHEN NOT MATCHED THEN
  INSERT (NoteHistoryKey, RegionKey, PracticeKey, Coid, PatientKey, Note, NoteType, NoteCreatedDate, NoteCreatedTime, NoteCreatedByUserKey, NoteCreatedByUserID, NoteLastUpdatedByUserKey, NoteLastUpdatedByUserID, SourcePrimaryKeyValue, NotesPK, PatientSourceGuid, ActiveFlag, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ClaimKey, ClaimNumber, CoderNoteFlag)
  VALUES (source.NoteHistoryKey, source.RegionKey, source.PracticeKey, TRIM(source.Coid), source.PatientKey, TRIM(source.Note), TRIM(source.NoteType), source.NoteCreatedDate, source.NoteCreatedTime, source.NoteCreatedByUserKey, TRIM(source.NoteCreatedByUserID), source.NoteLastUpdatedByUserKey, TRIM(source.NoteLastUpdatedByUserID), source.SourcePrimaryKeyValue, TRIM(source.NotesPK), TRIM(source.PatientSourceGuid), source.ActiveFlag, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.ClaimKey, source.ClaimNumber, source.CoderNoteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT NoteHistoryKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactNoteHistory
      GROUP BY NoteHistoryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactNoteHistory');
ELSE
  COMMIT TRANSACTION;
END IF;
