
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncReferral AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_JuncReferral AS source
ON target.ECWReferralKey = source.ECWReferralKey
WHEN MATCHED THEN
  UPDATE SET
  target.ECWReferralKey = source.ECWReferralKey,
 target.RegionKey = source.RegionKey,
 target.patientID = source.patientID,
 target.PatientKey = source.PatientKey,
 target.insId = source.insId,
 target.IplanKey = source.IplanKey,
 target.refFrom = source.refFrom,
 target.RefFromProviderKey = source.RefFromProviderKey,
 target.authNo = TRIM(source.authNo),
 target.Refdate = source.Refdate,
 target.reason = TRIM(source.reason),
 target.diagnosis = TRIM(source.diagnosis),
 target.refStDate = source.refStDate,
 target.refEnddate = source.refEnddate,
 target.visitsAllowed = source.visitsAllowed,
 target.visitsUsed = source.visitsUsed,
 target.RefTo = source.RefTo,
 target.RefToProviderKey = source.RefToProviderKey,
 target.notes = TRIM(source.notes),
 target.referralType = TRIM(source.referralType),
 target.priority = source.priority,
 target.assignedTo = TRIM(source.assignedTo),
 target.assignedToId = source.assignedToId,
 target.status = TRIM(source.status),
 target.authtype = TRIM(source.authtype),
 target.procedures = TRIM(source.procedures),
 target.fromfacility = source.fromfacility,
 target.FromFacilityKey = source.FromFacilityKey,
 target.ToFacility = source.ToFacility,
 target.ToFacilityKey = source.ToFacilityKey,
 target.speciality = source.speciality,
 target.POS = source.POS,
 target.UnitType = TRIM(source.UnitType),
 target.FrontOfficeAuth = source.FrontOfficeAuth,
 target.ReferralNumber = TRIM(source.ReferralNumber),
 target.apptDate = source.apptDate,
 target.clinicalNotes = TRIM(source.clinicalNotes),
 target.ReceivedDate = source.ReceivedDate,
 target.refEncId = source.refEncId,
 target.RefEncounterKey = source.RefEncounterKey,
 target.ApptTime = TRIM(source.ApptTime),
 target.extNHXApptBlockId = source.extNHXApptBlockId,
 target.extNHXRefTxId = source.extNHXRefTxId,
 target.refReqId = source.refReqId,
 target.refFromP2pNPI = TRIM(source.refFromP2pNPI),
 target.refFromName = TRIM(source.refFromName),
 target.refToP2pNPI = TRIM(source.refToP2pNPI),
 target.refToName = TRIM(source.refToName),
 target.uploadedToPtDocs = source.uploadedToPtDocs,
 target.deleteFlag = source.deleteFlag,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ECWReferralKey, RegionKey, patientID, PatientKey, insId, IplanKey, refFrom, RefFromProviderKey, authNo, Refdate, reason, diagnosis, refStDate, refEnddate, visitsAllowed, visitsUsed, RefTo, RefToProviderKey, notes, referralType, priority, assignedTo, assignedToId, status, authtype, procedures, fromfacility, FromFacilityKey, ToFacility, ToFacilityKey, speciality, POS, UnitType, FrontOfficeAuth, ReferralNumber, apptDate, clinicalNotes, ReceivedDate, refEncId, RefEncounterKey, ApptTime, extNHXApptBlockId, extNHXRefTxId, refReqId, refFromP2pNPI, refFromName, refToP2pNPI, refToName, uploadedToPtDocs, deleteFlag, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ECWReferralKey, source.RegionKey, source.patientID, source.PatientKey, source.insId, source.IplanKey, source.refFrom, source.RefFromProviderKey, TRIM(source.authNo), source.Refdate, TRIM(source.reason), TRIM(source.diagnosis), source.refStDate, source.refEnddate, source.visitsAllowed, source.visitsUsed, source.RefTo, source.RefToProviderKey, TRIM(source.notes), TRIM(source.referralType), source.priority, TRIM(source.assignedTo), source.assignedToId, TRIM(source.status), TRIM(source.authtype), TRIM(source.procedures), source.fromfacility, source.FromFacilityKey, source.ToFacility, source.ToFacilityKey, source.speciality, source.POS, TRIM(source.UnitType), source.FrontOfficeAuth, TRIM(source.ReferralNumber), source.apptDate, TRIM(source.clinicalNotes), source.ReceivedDate, source.refEncId, source.RefEncounterKey, TRIM(source.ApptTime), source.extNHXApptBlockId, source.extNHXRefTxId, source.refReqId, TRIM(source.refFromP2pNPI), TRIM(source.refFromName), TRIM(source.refToP2pNPI), TRIM(source.refToName), source.uploadedToPtDocs, source.deleteFlag, source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ECWReferralKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_JuncReferral
      GROUP BY ECWReferralKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_JuncReferral');
ELSE
  COMMIT TRANSACTION;
END IF;
