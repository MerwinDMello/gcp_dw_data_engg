
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncInterfaceClaim AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_JuncInterfaceClaim AS source
ON target.EcwInterfaceKey = source.EcwInterfaceKey
WHEN MATCHED THEN
  UPDATE SET
  target.EcwInterfaceKey = source.EcwInterfaceKey,
 target.ClaimKey = source.ClaimKey,
 target.RegionKey = source.RegionKey,
 target.MessageId = source.MessageId,
 target.PMId = source.PMId,
 target.DateReceived = source.DateReceived,
 target.DateModified = source.DateModified,
 target.MessageContent = TRIM(source.MessageContent),
 target.ErrorMessage = TRIM(source.ErrorMessage),
 target.MessageType = TRIM(source.MessageType),
 target.PatientLastName = TRIM(source.PatientLastName),
 target.PatientFirstName = TRIM(source.PatientFirstName),
 target.PatientDOB = TRIM(source.PatientDOB),
 target.PatientAccountNumber = TRIM(source.PatientAccountNumber),
 target.VisitNumber = TRIM(source.VisitNumber),
 target.MessageStatus = source.MessageStatus,
 target.MessageControlId = TRIM(source.MessageControlId),
 target.MFNMsgType = TRIM(source.MFNMsgType),
 target.MFNId = TRIM(source.MFNId),
 target.ProcessFlow = TRIM(source.ProcessFlow),
 target.PatientId = source.PatientId,
 target.EcwEncounterId = source.EcwEncounterId,
 target.MesssageContentId = source.MesssageContentId,
 target.PMListId = source.PMListId,
 target.PMListPMId = source.PMListPMId,
 target.PMListName = TRIM(source.PMListName),
 target.DeleteFlag = source.DeleteFlag,
 target.InterfaceConfigured = source.InterfaceConfigured,
 target.InsId = source.InsId,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (EcwInterfaceKey, ClaimKey, RegionKey, MessageId, PMId, DateReceived, DateModified, MessageContent, ErrorMessage, MessageType, PatientLastName, PatientFirstName, PatientDOB, PatientAccountNumber, VisitNumber, MessageStatus, MessageControlId, MFNMsgType, MFNId, ProcessFlow, PatientId, EcwEncounterId, MesssageContentId, PMListId, PMListPMId, PMListName, DeleteFlag, InterfaceConfigured, InsId, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.EcwInterfaceKey, source.ClaimKey, source.RegionKey, source.MessageId, source.PMId, source.DateReceived, source.DateModified, TRIM(source.MessageContent), TRIM(source.ErrorMessage), TRIM(source.MessageType), TRIM(source.PatientLastName), TRIM(source.PatientFirstName), TRIM(source.PatientDOB), TRIM(source.PatientAccountNumber), TRIM(source.VisitNumber), source.MessageStatus, TRIM(source.MessageControlId), TRIM(source.MFNMsgType), TRIM(source.MFNId), TRIM(source.ProcessFlow), source.PatientId, source.EcwEncounterId, source.MesssageContentId, source.PMListId, source.PMListPMId, TRIM(source.PMListName), source.DeleteFlag, source.InterfaceConfigured, source.InsId, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EcwInterfaceKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_JuncInterfaceClaim
      GROUP BY EcwInterfaceKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_JuncInterfaceClaim');
ELSE
  COMMIT TRANSACTION;
END IF;
