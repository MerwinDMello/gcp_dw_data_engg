
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PK_FactSkilledNursingFacilityNote AS target
USING {{ params.param_psc_stage_dataset_name }}.PK_FactSkilledNursingFacilityNote AS source
ON target.PKSkilledNursingFacilityNoteKey = source.PKSkilledNursingFacilityNoteKey
WHEN MATCHED THEN
  UPDATE SET
  target.PKSkilledNursingFacilityNoteKey = source.PKSkilledNursingFacilityNoteKey,
 target.Coid = TRIM(source.Coid),
 target.PKRegionName = TRIM(source.PKRegionName),
 target.NoteTypeName = TRIM(source.NoteTypeName),
 target.SOURCE = TRIM(source.SOURCE),
 target.VisitLocationFacility = TRIM(source.VisitLocationFacility),
 target.PKFinancialNumber = TRIM(source.PKFinancialNumber),
 target.PKEncounterKey = source.PKEncounterKey,
 target.PKEncounterId = source.PKEncounterId,
 target.PatientId = source.PatientId,
 target.PatientLastName = TRIM(source.PatientLastName),
 target.PatientFirstName = TRIM(source.PatientFirstName),
 target.PatientMiddleName = TRIM(source.PatientMiddleName),
 target.PatientMRN = TRIM(source.PatientMRN),
 target.ChargeTransactionid = source.ChargeTransactionid,
 target.NoteCreatedBy = TRIM(source.NoteCreatedBy),
 target.NoteModifiedBy = TRIM(source.NoteModifiedBy),
 target.NoteCreatedDate = source.NoteCreatedDate,
 target.NoteDate = source.NoteDate,
 target.SignedStatus = TRIM(source.SignedStatus),
 target.SignedDate = source.SignedDate,
 target.NoteLastEditDate = source.NoteLastEditDate,
 target.DeleteFlag = source.DeleteFlag,
 target.SyncCreatedTime = source.SyncCreatedTime,
 target.SyncModifiedTime = source.SyncModifiedTime,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (PKSkilledNursingFacilityNoteKey, Coid, PKRegionName, NoteTypeName, SOURCE, VisitLocationFacility, PKFinancialNumber, PKEncounterKey, PKEncounterId, PatientId, PatientLastName, PatientFirstName, PatientMiddleName, PatientMRN, ChargeTransactionid, NoteCreatedBy, NoteModifiedBy, NoteCreatedDate, NoteDate, SignedStatus, SignedDate, NoteLastEditDate, DeleteFlag, SyncCreatedTime, SyncModifiedTime, SourceAPrimaryKeyValue, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.PKSkilledNursingFacilityNoteKey, TRIM(source.Coid), TRIM(source.PKRegionName), TRIM(source.NoteTypeName), TRIM(source.SOURCE), TRIM(source.VisitLocationFacility), TRIM(source.PKFinancialNumber), source.PKEncounterKey, source.PKEncounterId, source.PatientId, TRIM(source.PatientLastName), TRIM(source.PatientFirstName), TRIM(source.PatientMiddleName), TRIM(source.PatientMRN), source.ChargeTransactionid, TRIM(source.NoteCreatedBy), TRIM(source.NoteModifiedBy), source.NoteCreatedDate, source.NoteDate, TRIM(source.SignedStatus), source.SignedDate, source.NoteLastEditDate, source.DeleteFlag, source.SyncCreatedTime, source.SyncModifiedTime, source.SourceAPrimaryKeyValue, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PKSkilledNursingFacilityNoteKey
      FROM {{ params.param_psc_core_dataset_name }}.PK_FactSkilledNursingFacilityNote
      GROUP BY PKSkilledNursingFacilityNoteKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PK_FactSkilledNursingFacilityNote');
ELSE
  COMMIT TRANSACTION;
END IF;
