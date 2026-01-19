
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterMedaxionCodingStatus AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactEncounterMedaxionCodingStatus AS source
ON target.MedaxionCodingStatusKey = source.MedaxionCodingStatusKey
WHEN MATCHED THEN
  UPDATE SET
  target.MedaxionCodingStatusKey = source.MedaxionCodingStatusKey,
 target.MedaxionRegionName = TRIM(source.MedaxionRegionName),
 target.MedaxionLocation = TRIM(source.MedaxionLocation),
 target.PracticeName = TRIM(source.PracticeName),
 target.Department = TRIM(source.Department),
 target.ProviderName = TRIM(source.ProviderName),
 target.ProviderType = TRIM(source.ProviderType),
 target.ServiceDateKey = source.ServiceDateKey,
 target.CaseNumberFIN = TRIM(source.CaseNumberFIN),
 target.PatientMRN = TRIM(source.PatientMRN),
 target.PatientFirst3 = TRIM(source.PatientFirst3),
 target.PatientLast3 = TRIM(source.PatientLast3),
 target.PatientDOB = source.PatientDOB,
 target.FacilityCOID = TRIM(source.FacilityCOID),
 target.POSCode = TRIM(source.POSCode),
 target.SurgeonOfRecordName = TRIM(source.SurgeonOfRecordName),
 target.CodingCompletedDate = source.CodingCompletedDate,
 target.CodingCompletedDateTime = source.CodingCompletedDateTime,
 target.CoderName = TRIM(source.CoderName),
 target.EmergencyFlag = source.EmergencyFlag,
 target.ASA = TRIM(source.ASA),
 target.AuditNoteStatus = TRIM(source.AuditNoteStatus),
 target.AnesthesiaStartIntraOp = source.AnesthesiaStartIntraOp,
 target.AnesthesiaStopPostOp = source.AnesthesiaStopPostOp,
 target.CrossesMidnightFlag = source.CrossesMidnightFlag,
 target.PulseCaseLink = TRIM(source.PulseCaseLink),
 target.CaseEncounterType = TRIM(source.CaseEncounterType),
 target.PulseEncounterLink = TRIM(source.PulseEncounterLink),
 target.CreatedDate = TRIM(source.CreatedDate),
 target.LastSignedDate = TRIM(source.LastSignedDate),
 target.LastAction = TRIM(source.LastAction),
 target.LastCoder = TRIM(source.LastCoder),
 target.LastQueue = TRIM(source.LastQueue),
 target.LastActionDateTime = source.LastActionDateTime,
 target.Box19Note = TRIM(source.Box19Note),
 target.CPTCodeText = TRIM(source.CPTCodeText),
 target.SyntheticCaseID = TRIM(source.SyntheticCaseID),
 target.RcmSyntheticEncounterID = TRIM(source.RcmSyntheticEncounterID),
 target.FirstFileDate = source.FirstFileDate,
 target.LastFileDate = source.LastFileDate,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (MedaxionCodingStatusKey, MedaxionRegionName, MedaxionLocation, PracticeName, Department, ProviderName, ProviderType, ServiceDateKey, CaseNumberFIN, PatientMRN, PatientFirst3, PatientLast3, PatientDOB, FacilityCOID, POSCode, SurgeonOfRecordName, CodingCompletedDate, CodingCompletedDateTime, CoderName, EmergencyFlag, ASA, AuditNoteStatus, AnesthesiaStartIntraOp, AnesthesiaStopPostOp, CrossesMidnightFlag, PulseCaseLink, CaseEncounterType, PulseEncounterLink, CreatedDate, LastSignedDate, LastAction, LastCoder, LastQueue, LastActionDateTime, Box19Note, CPTCodeText, SyntheticCaseID, RcmSyntheticEncounterID, FirstFileDate, LastFileDate, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.MedaxionCodingStatusKey, TRIM(source.MedaxionRegionName), TRIM(source.MedaxionLocation), TRIM(source.PracticeName), TRIM(source.Department), TRIM(source.ProviderName), TRIM(source.ProviderType), source.ServiceDateKey, TRIM(source.CaseNumberFIN), TRIM(source.PatientMRN), TRIM(source.PatientFirst3), TRIM(source.PatientLast3), source.PatientDOB, TRIM(source.FacilityCOID), TRIM(source.POSCode), TRIM(source.SurgeonOfRecordName), source.CodingCompletedDate, source.CodingCompletedDateTime, TRIM(source.CoderName), source.EmergencyFlag, TRIM(source.ASA), TRIM(source.AuditNoteStatus), source.AnesthesiaStartIntraOp, source.AnesthesiaStopPostOp, source.CrossesMidnightFlag, TRIM(source.PulseCaseLink), TRIM(source.CaseEncounterType), TRIM(source.PulseEncounterLink), TRIM(source.CreatedDate), TRIM(source.LastSignedDate), TRIM(source.LastAction), TRIM(source.LastCoder), TRIM(source.LastQueue), source.LastActionDateTime, TRIM(source.Box19Note), TRIM(source.CPTCodeText), TRIM(source.SyntheticCaseID), TRIM(source.RcmSyntheticEncounterID), source.FirstFileDate, source.LastFileDate, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT MedaxionCodingStatusKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterMedaxionCodingStatus
      GROUP BY MedaxionCodingStatusKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterMedaxionCodingStatus');
ELSE
  COMMIT TRANSACTION;
END IF;
