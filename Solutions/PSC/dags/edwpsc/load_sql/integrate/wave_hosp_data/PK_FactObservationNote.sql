
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PK_FactObservationNote AS target
USING {{ params.param_psc_stage_dataset_name }}.PK_FactObservationNote AS source
ON target.PKObservationNoteKey = source.PKObservationNoteKey
WHEN MATCHED THEN
  UPDATE SET
  target.PKObservationNoteKey = source.PKObservationNoteKey,
 target.PKRegionName = TRIM(source.PKRegionName),
 target.PKEncounterKey = source.PKEncounterKey,
 target.PKEncounterId = source.PKEncounterId,
 target.NoteTypeName = TRIM(source.NoteTypeName),
 target.NoteCreateDateTime = source.NoteCreateDateTime,
 target.NoteUpdateDateTime = source.NoteUpdateDateTime,
 target.NoteDeleteFlag = source.NoteDeleteFlag,
 target.NoteStatus = TRIM(source.NoteStatus),
 target.NoteProviderLastName = TRIM(source.NoteProviderLastName),
 target.NoteProviderFirstName = TRIM(source.NoteProviderFirstName),
 target.NoteProviderNPI = TRIM(source.NoteProviderNPI),
 target.NoteProviderUserName = TRIM(source.NoteProviderUserName),
 target.ProviderLastName = TRIM(source.ProviderLastName),
 target.ProviderFirstName = TRIM(source.ProviderFirstName),
 target.ProviderNPI = TRIM(source.ProviderNPI),
 target.ProviderUserName = TRIM(source.ProviderUserName),
 target.AuthorLastName = TRIM(source.AuthorLastName),
 target.AuthorFirstName = TRIM(source.AuthorFirstName),
 target.AuthorNPI = TRIM(source.AuthorNPI),
 target.AuthorUserName = TRIM(source.AuthorUserName),
 target.ObservationDeleteFlag = source.ObservationDeleteFlag,
 target.ObservationCreateDateTime = source.ObservationCreateDateTime,
 target.ObservationUpdateDateTime = source.ObservationUpdateDateTime,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.ReportTitle = TRIM(source.ReportTitle),
 target.OriginalPKEncounterKey = source.OriginalPKEncounterKey,
 target.OriginalPKEncounterId = source.OriginalPKEncounterId,
 target.PKFinancialNumber = TRIM(source.PKFinancialNumber),
 target.NoteProviderId = source.NoteProviderId,
 target.ProviderId = source.ProviderId,
 target.AuthorId = source.AuthorId
WHEN NOT MATCHED THEN
  INSERT (PKObservationNoteKey, PKRegionName, PKEncounterKey, PKEncounterId, NoteTypeName, NoteCreateDateTime, NoteUpdateDateTime, NoteDeleteFlag, NoteStatus, NoteProviderLastName, NoteProviderFirstName, NoteProviderNPI, NoteProviderUserName, ProviderLastName, ProviderFirstName, ProviderNPI, ProviderUserName, AuthorLastName, AuthorFirstName, AuthorNPI, AuthorUserName, ObservationDeleteFlag, ObservationCreateDateTime, ObservationUpdateDateTime, SourceAPrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ReportTitle, OriginalPKEncounterKey, OriginalPKEncounterId, PKFinancialNumber, NoteProviderId, ProviderId, AuthorId)
  VALUES (source.PKObservationNoteKey, TRIM(source.PKRegionName), source.PKEncounterKey, source.PKEncounterId, TRIM(source.NoteTypeName), source.NoteCreateDateTime, source.NoteUpdateDateTime, source.NoteDeleteFlag, TRIM(source.NoteStatus), TRIM(source.NoteProviderLastName), TRIM(source.NoteProviderFirstName), TRIM(source.NoteProviderNPI), TRIM(source.NoteProviderUserName), TRIM(source.ProviderLastName), TRIM(source.ProviderFirstName), TRIM(source.ProviderNPI), TRIM(source.ProviderUserName), TRIM(source.AuthorLastName), TRIM(source.AuthorFirstName), TRIM(source.AuthorNPI), TRIM(source.AuthorUserName), source.ObservationDeleteFlag, source.ObservationCreateDateTime, source.ObservationUpdateDateTime, source.SourceAPrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.ReportTitle), source.OriginalPKEncounterKey, source.OriginalPKEncounterId, TRIM(source.PKFinancialNumber), source.NoteProviderId, source.ProviderId, source.AuthorId);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PKObservationNoteKey
      FROM {{ params.param_psc_core_dataset_name }}.PK_FactObservationNote
      GROUP BY PKObservationNoteKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PK_FactObservationNote');
ELSE
  COMMIT TRANSACTION;
END IF;
