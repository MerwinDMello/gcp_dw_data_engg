
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactDFTMessage AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactDFTMessage AS source
ON target.DFTMessageKey = source.DFTMessageKey
WHEN MATCHED THEN
  UPDATE SET
  target.DFTMessageKey = source.DFTMessageKey,
 target.RegionKey = source.RegionKey,
 target.DateReceived = source.DateReceived,
 target.DateModified = source.DateModified,
 target.PatientAccountNumber = TRIM(source.PatientAccountNumber),
 target.PatientLastName = TRIM(source.PatientLastName),
 target.PatientFirstName = TRIM(source.PatientFirstName),
 target.PatientDOBKey = source.PatientDOBKey,
 target.MessageStatus = source.MessageStatus,
 target.VisitNumber = TRIM(source.VisitNumber),
 target.EncounterId = TRIM(source.EncounterId),
 target.EncounterKey = source.EncounterKey,
 target.ClaimId = source.ClaimId,
 target.ClaimKey = source.ClaimKey,
 target.ErrorMessage = TRIM(source.ErrorMessage),
 target.MessageControlId = TRIM(source.MessageControlId),
 target.ProviderId = source.ProviderId,
 target.ProviderKey = source.ProviderKey,
 target.FacilityId = source.FacilityId,
 target.FacilityKey = source.FacilityKey,
 target.SentId = source.SentId,
 target.MessageType = TRIM(source.MessageType),
 target.ReconciledFlag = source.ReconciledFlag,
 target.PracticeId = TRIM(source.PracticeId),
 target.PmId = source.PmId,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.MessageControlIdShort = TRIM(source.MessageControlIdShort),
 target.PmIdName = TRIM(source.PmIdName),
 target.ChargeTransactionID = TRIM(source.ChargeTransactionID),
 target.SendingFacility = TRIM(source.SendingFacility),
 target.VisitLocationFacility = TRIM(source.VisitLocationFacility),
 target.VisitLocationUnit = TRIM(source.VisitLocationUnit),
 target.VisitLocation = TRIM(source.VisitLocation),
 target.COID = TRIM(source.COID)
WHEN NOT MATCHED THEN
  INSERT (DFTMessageKey, RegionKey, DateReceived, DateModified, PatientAccountNumber, PatientLastName, PatientFirstName, PatientDOBKey, MessageStatus, VisitNumber, EncounterId, EncounterKey, ClaimId, ClaimKey, ErrorMessage, MessageControlId, ProviderId, ProviderKey, FacilityId, FacilityKey, SentId, MessageType, ReconciledFlag, PracticeId, PmId, SourceAPrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, MessageControlIdShort, PmIdName, ChargeTransactionID, SendingFacility, VisitLocationFacility, VisitLocationUnit, VisitLocation, COID)
  VALUES (source.DFTMessageKey, source.RegionKey, source.DateReceived, source.DateModified, TRIM(source.PatientAccountNumber), TRIM(source.PatientLastName), TRIM(source.PatientFirstName), source.PatientDOBKey, source.MessageStatus, TRIM(source.VisitNumber), TRIM(source.EncounterId), source.EncounterKey, source.ClaimId, source.ClaimKey, TRIM(source.ErrorMessage), TRIM(source.MessageControlId), source.ProviderId, source.ProviderKey, source.FacilityId, source.FacilityKey, source.SentId, TRIM(source.MessageType), source.ReconciledFlag, TRIM(source.PracticeId), source.PmId, source.SourceAPrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.MessageControlIdShort), TRIM(source.PmIdName), TRIM(source.ChargeTransactionID), TRIM(source.SendingFacility), TRIM(source.VisitLocationFacility), TRIM(source.VisitLocationUnit), TRIM(source.VisitLocation), TRIM(source.COID));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT DFTMessageKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactDFTMessage
      GROUP BY DFTMessageKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactDFTMessage');
ELSE
  COMMIT TRANSACTION;
END IF;
