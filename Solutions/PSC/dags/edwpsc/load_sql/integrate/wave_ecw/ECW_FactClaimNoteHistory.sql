
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactClaimNoteHistory AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactClaimNoteHistory AS source
ON target.ClaimStatusNoteKey = source.ClaimStatusNoteKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimStatusNoteKey = source.ClaimStatusNoteKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.ClaimNote = TRIM(source.ClaimNote),
 target.ClaimNoteCreatedDate = source.ClaimNoteCreatedDate,
 target.ClaimNoteCreatedTime = source.ClaimNoteCreatedTime,
 target.ClaimNoteCreatedByUserKey = source.ClaimNoteCreatedByUserKey,
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
  INSERT (ClaimStatusNoteKey, ClaimKey, ClaimNumber, RegionKey, Coid, ClaimNote, ClaimNoteCreatedDate, ClaimNoteCreatedTime, ClaimNoteCreatedByUserKey, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ArchivedRecord)
  VALUES (source.ClaimStatusNoteKey, source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.Coid), TRIM(source.ClaimNote), source.ClaimNoteCreatedDate, source.ClaimNoteCreatedTime, source.ClaimNoteCreatedByUserKey, source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.ArchivedRecord));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimStatusNoteKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactClaimNoteHistory
      GROUP BY ClaimStatusNoteKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactClaimNoteHistory');
ELSE
  COMMIT TRANSACTION;
END IF;
