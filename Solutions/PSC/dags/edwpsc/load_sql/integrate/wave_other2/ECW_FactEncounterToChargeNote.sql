
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterToChargeNote AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactEncounterToChargeNote AS source
ON target.NoteEncounterToChargeKey = source.NoteEncounterToChargeKey
WHEN MATCHED THEN
  UPDATE SET
  target.NoteEncounterToChargeKey = source.NoteEncounterToChargeKey,
 target.EncounterToChargeKey = source.EncounterToChargeKey,
 target.NoteName = TRIM(source.NoteName),
 target.NoteStatus = TRIM(source.NoteStatus),
 target.NotePriority = source.NotePriority,
 target.NoteCloseToMidnight = TRIM(source.NoteCloseToMidnight),
 target.NoteCreateDTM = source.NoteCreateDTM,
 target.NoteUpdateDTM = source.NoteUpdateDTM,
 target.NoteProviderSignedNPI = TRIM(source.NoteProviderSignedNPI),
 target.NoteCosignProviderNPI = TRIM(source.NoteCosignProviderNPI),
 target.NoteServiceDateKey = source.NoteServiceDateKey,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (NoteEncounterToChargeKey, EncounterToChargeKey, NoteName, NoteStatus, NotePriority, NoteCloseToMidnight, NoteCreateDTM, NoteUpdateDTM, NoteProviderSignedNPI, NoteCosignProviderNPI, NoteServiceDateKey, SourcePrimaryKeyValue, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.NoteEncounterToChargeKey, source.EncounterToChargeKey, TRIM(source.NoteName), TRIM(source.NoteStatus), source.NotePriority, TRIM(source.NoteCloseToMidnight), source.NoteCreateDTM, source.NoteUpdateDTM, TRIM(source.NoteProviderSignedNPI), TRIM(source.NoteCosignProviderNPI), source.NoteServiceDateKey, source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT NoteEncounterToChargeKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterToChargeNote
      GROUP BY NoteEncounterToChargeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterToChargeNote');
ELSE
  COMMIT TRANSACTION;
END IF;
