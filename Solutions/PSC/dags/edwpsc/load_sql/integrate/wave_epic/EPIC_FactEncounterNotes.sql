
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactEncounterNotes AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactEncounterNotes AS source
ON target.EncounterNotesKey = source.EncounterNotesKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncounterNotesKey = source.EncounterNotesKey,
 target.RegionKey = source.RegionKey,
 target.EncounterKey = source.EncounterKey,
 target.EncounterId = source.EncounterId,
 target.ClaimKey = source.ClaimKey,
 target.VisitNumber = source.VisitNumber,
 target.NoteType = TRIM(source.NoteType),
 target.NoteDateKey = source.NoteDateKey,
 target.NoteUserKey = source.NoteUserKey,
 target.EncounterNote = TRIM(source.EncounterNote),
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.PayToProviderKey = source.PayToProviderKey,
 target.PatientKey = source.PatientKey,
 target.ServiceAreaKey = source.ServiceAreaKey,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceBPrimaryKeyValue = source.SourceBPrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (EncounterNotesKey, RegionKey, EncounterKey, EncounterId, ClaimKey, VisitNumber, NoteType, NoteDateKey, NoteUserKey, EncounterNote, ServicingProviderKey, PayToProviderKey, PatientKey, ServiceAreaKey, SourceAPrimaryKeyValue, SourceBPrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.EncounterNotesKey, source.RegionKey, source.EncounterKey, source.EncounterId, source.ClaimKey, source.VisitNumber, TRIM(source.NoteType), source.NoteDateKey, source.NoteUserKey, TRIM(source.EncounterNote), source.ServicingProviderKey, source.PayToProviderKey, source.PatientKey, source.ServiceAreaKey, source.SourceAPrimaryKeyValue, source.SourceBPrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterNotesKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactEncounterNotes
      GROUP BY EncounterNotesKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactEncounterNotes');
ELSE
  COMMIT TRANSACTION;
END IF;
