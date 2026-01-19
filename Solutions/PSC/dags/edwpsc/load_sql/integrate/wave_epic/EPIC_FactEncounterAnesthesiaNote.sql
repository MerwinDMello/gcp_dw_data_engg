
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactEncounterAnesthesiaNote AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactEncounterAnesthesiaNote AS source
ON target.AnesthesiaNoteKey = source.AnesthesiaNoteKey
WHEN MATCHED THEN
  UPDATE SET
  target.AnesthesiaNoteKey = source.AnesthesiaNoteKey,
 target.AnesthesiaKey = source.AnesthesiaKey,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.NoteTypeCode = TRIM(source.NoteTypeCode),
 target.NoteTypeAbbreviation = TRIM(source.NoteTypeAbbreviation),
 target.NoteTypeName = TRIM(source.NoteTypeName),
 target.NoteReason = TRIM(source.NoteReason),
 target.NoteProviderId = source.NoteProviderId,
 target.NoteProviderKey = source.NoteProviderKey,
 target.NoteProviderName = TRIM(source.NoteProviderName),
 target.NoteUserId = source.NoteUserId,
 target.NoteUserKey = source.NoteUserKey,
 target.NoteUserName = TRIM(source.NoteUserName),
 target.PatientId = source.PatientId,
 target.PatientKey = source.PatientKey,
 target.NoteCreateDateTime = source.NoteCreateDateTime,
 target.NoteUpdateDateTime = source.NoteUpdateDateTime,
 target.NoteSignedFlag = source.NoteSignedFlag,
 target.NoteSignedDateTime = source.NoteSignedDateTime,
 target.EncounterId = source.EncounterId,
 target.EncounterKey = source.EncounterKey,
 target.NoteServiceDateKey = source.NoteServiceDateKey,
 target.NoteWriterSavedFlag = source.NoteWriterSavedFlag,
 target.ArchivedNoteFlag = source.ArchivedNoteFlag,
 target.NoteDeleteFlag = source.NoteDeleteFlag,
 target.NoteDeleteDateTime = source.NoteDeleteDateTime,
 target.NoteDeleteUserId = source.NoteDeleteUserId,
 target.NoteDeleteUserKey = source.NoteDeleteUserKey,
 target.NoteDeleteUserName = TRIM(source.NoteDeleteUserName),
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (AnesthesiaNoteKey, AnesthesiaKey, RegionKey, Coid, NoteTypeCode, NoteTypeAbbreviation, NoteTypeName, NoteReason, NoteProviderId, NoteProviderKey, NoteProviderName, NoteUserId, NoteUserKey, NoteUserName, PatientId, PatientKey, NoteCreateDateTime, NoteUpdateDateTime, NoteSignedFlag, NoteSignedDateTime, EncounterId, EncounterKey, NoteServiceDateKey, NoteWriterSavedFlag, ArchivedNoteFlag, NoteDeleteFlag, NoteDeleteDateTime, NoteDeleteUserId, NoteDeleteUserKey, NoteDeleteUserName, SourceAPrimaryKeyValue, SourceSystemCode, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.AnesthesiaNoteKey, source.AnesthesiaKey, source.RegionKey, TRIM(source.Coid), TRIM(source.NoteTypeCode), TRIM(source.NoteTypeAbbreviation), TRIM(source.NoteTypeName), TRIM(source.NoteReason), source.NoteProviderId, source.NoteProviderKey, TRIM(source.NoteProviderName), source.NoteUserId, source.NoteUserKey, TRIM(source.NoteUserName), source.PatientId, source.PatientKey, source.NoteCreateDateTime, source.NoteUpdateDateTime, source.NoteSignedFlag, source.NoteSignedDateTime, source.EncounterId, source.EncounterKey, source.NoteServiceDateKey, source.NoteWriterSavedFlag, source.ArchivedNoteFlag, source.NoteDeleteFlag, source.NoteDeleteDateTime, source.NoteDeleteUserId, source.NoteDeleteUserKey, TRIM(source.NoteDeleteUserName), source.SourceAPrimaryKeyValue, TRIM(source.SourceSystemCode), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT AnesthesiaNoteKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactEncounterAnesthesiaNote
      GROUP BY AnesthesiaNoteKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactEncounterAnesthesiaNote');
ELSE
  COMMIT TRANSACTION;
END IF;
