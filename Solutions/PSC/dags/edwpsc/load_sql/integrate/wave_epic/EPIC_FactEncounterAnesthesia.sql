
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactEncounterAnesthesia AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactEncounterAnesthesia AS source
ON target.AnesthesiaKey = source.AnesthesiaKey
WHEN MATCHED THEN
  UPDATE SET
  target.AnesthesiaKey = source.AnesthesiaKey,
 target.RegionKey = source.RegionKey,
 target.AnesthesiaLocationName = TRIM(source.AnesthesiaLocationName),
 target.EpisodeId = source.EpisodeId,
 target.ServiceDateKey = source.ServiceDateKey,
 target.CaseNumberFIN = source.CaseNumberFIN,
 target.PatientKey = source.PatientKey,
 target.PatientMRN = TRIM(source.PatientMRN),
 target.PatientLastName = TRIM(source.PatientLastName),
 target.PatientFirstName = TRIM(source.PatientFirstName),
 target.ProcedureName = TRIM(source.ProcedureName),
 target.AnesthesiaStartTime = source.AnesthesiaStartTime,
 target.AnesthesiaEndTime = source.AnesthesiaEndTime,
 target.Anesthesiologist = TRIM(source.Anesthesiologist),
 target.AnesthesiaStaff = TRIM(source.AnesthesiaStaff),
 target.AnesRecSignedFlag = source.AnesRecSignedFlag,
 target.AnesRecSignedDateTime = source.AnesRecSignedDateTime,
 target.PreOpNoteFlag = source.PreOpNoteFlag,
 target.PreOpNoteSignedDateTime = source.PreOpNoteSignedDateTime,
 target.PreOpNoteProviderName = TRIM(source.PreOpNoteProviderName),
 target.PostOpNoteFlag = source.PostOpNoteFlag,
 target.PostOpNoteSignedDateTime = source.PostOpNoteSignedDateTime,
 target.PostOpNoteProviderName = TRIM(source.PostOpNoteProviderName),
 target.EncounterClosedFlag = source.EncounterClosedFlag,
 target.EventCanceledFlag = source.EventCanceledFlag,
 target.EventCanceledStatus = TRIM(source.EventCanceledStatus),
 target.EventCanceledDateTime = source.EventCanceledDateTime,
 target.LogOrCaseName = TRIM(source.LogOrCaseName),
 target.CaseCanceledFlag = source.CaseCanceledFlag,
 target.CaseCanceledReason = TRIM(source.CaseCanceledReason),
 target.Comments = TRIM(source.Comments),
 target.SourceRecordUpdateDateTime = source.SourceRecordUpdateDateTime,
 target.Anesthesia53EncounterKey = source.Anesthesia53EncounterKey,
 target.Anesthesia53EncounterId = source.Anesthesia53EncounterId,
 target.Anesthesia52EncounterKey = source.Anesthesia52EncounterKey,
 target.Anesthesia52EncounterId = source.Anesthesia52EncounterId,
 target.EpicPatientId = TRIM(source.EpicPatientId),
 target.CaseId = TRIM(source.CaseId),
 target.LogId = TRIM(source.LogId),
 target.AnesthesiologistProviderKey = source.AnesthesiologistProviderKey,
 target.AnesthesiologistProviderId = source.AnesthesiologistProviderId,
 target.PreOpNoteProviderId = source.PreOpNoteProviderId,
 target.PreOpNoteProviderKey = source.PreOpNoteProviderKey,
 target.PostOpNoteProviderId = source.PostOpNoteProviderId,
 target.PostOpNoteProviderKey = source.PostOpNoteProviderKey,
 target.CaseLOCKey = source.CaseLOCKey,
 target.LogLOCKey = source.LogLOCKey,
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (AnesthesiaKey, RegionKey, AnesthesiaLocationName, EpisodeId, ServiceDateKey, CaseNumberFIN, PatientKey, PatientMRN, PatientLastName, PatientFirstName, ProcedureName, AnesthesiaStartTime, AnesthesiaEndTime, Anesthesiologist, AnesthesiaStaff, AnesRecSignedFlag, AnesRecSignedDateTime, PreOpNoteFlag, PreOpNoteSignedDateTime, PreOpNoteProviderName, PostOpNoteFlag, PostOpNoteSignedDateTime, PostOpNoteProviderName, EncounterClosedFlag, EventCanceledFlag, EventCanceledStatus, EventCanceledDateTime, LogOrCaseName, CaseCanceledFlag, CaseCanceledReason, Comments, SourceRecordUpdateDateTime, Anesthesia53EncounterKey, Anesthesia53EncounterId, Anesthesia52EncounterKey, Anesthesia52EncounterId, EpicPatientId, CaseId, LogId, AnesthesiologistProviderKey, AnesthesiologistProviderId, PreOpNoteProviderId, PreOpNoteProviderKey, PostOpNoteProviderId, PostOpNoteProviderKey, CaseLOCKey, LogLOCKey, SourceAPrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.AnesthesiaKey, source.RegionKey, TRIM(source.AnesthesiaLocationName), source.EpisodeId, source.ServiceDateKey, source.CaseNumberFIN, source.PatientKey, TRIM(source.PatientMRN), TRIM(source.PatientLastName), TRIM(source.PatientFirstName), TRIM(source.ProcedureName), source.AnesthesiaStartTime, source.AnesthesiaEndTime, TRIM(source.Anesthesiologist), TRIM(source.AnesthesiaStaff), source.AnesRecSignedFlag, source.AnesRecSignedDateTime, source.PreOpNoteFlag, source.PreOpNoteSignedDateTime, TRIM(source.PreOpNoteProviderName), source.PostOpNoteFlag, source.PostOpNoteSignedDateTime, TRIM(source.PostOpNoteProviderName), source.EncounterClosedFlag, source.EventCanceledFlag, TRIM(source.EventCanceledStatus), source.EventCanceledDateTime, TRIM(source.LogOrCaseName), source.CaseCanceledFlag, TRIM(source.CaseCanceledReason), TRIM(source.Comments), source.SourceRecordUpdateDateTime, source.Anesthesia53EncounterKey, source.Anesthesia53EncounterId, source.Anesthesia52EncounterKey, source.Anesthesia52EncounterId, TRIM(source.EpicPatientId), TRIM(source.CaseId), TRIM(source.LogId), source.AnesthesiologistProviderKey, source.AnesthesiologistProviderId, source.PreOpNoteProviderId, source.PreOpNoteProviderKey, source.PostOpNoteProviderId, source.PostOpNoteProviderKey, source.CaseLOCKey, source.LogLOCKey, TRIM(source.SourceAPrimaryKeyValue), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT AnesthesiaKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactEncounterAnesthesia
      GROUP BY AnesthesiaKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactEncounterAnesthesia');
ELSE
  COMMIT TRANSACTION;
END IF;
