
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.OpenConnect_FactSourceCACProCharge AS target
USING {{ params.param_psc_stage_dataset_name }}.OpenConnect_FactSourceCACProCharge AS source
ON target.CACProChargeKey = source.CACProChargeKey
WHEN MATCHED THEN
  UPDATE SET
  target.CACProChargeKey = source.CACProChargeKey,
 target.MessageDateTime = source.MessageDateTime,
 target.BatchDate = source.BatchDate,
 target.DocumentId = source.DocumentId,
 target.OperatorId = TRIM(source.OperatorId),
 target.PatientSourceMRN = TRIM(source.PatientSourceMRN),
 target.PatientLastName = TRIM(source.PatientLastName),
 target.PatientFirstName = TRIM(source.PatientFirstName),
 target.PatientDateOfBirth = source.PatientDateOfBirth,
 target.VisitNumber = TRIM(source.VisitNumber),
 target.AttendingID = source.AttendingID,
 target.AttendingName = TRIM(source.AttendingName),
 target.ReferringId = source.ReferringId,
 target.ReferringName = TRIM(source.ReferringName),
 target.HospitalService = TRIM(source.HospitalService),
 target.VisitNumberCACPro = TRIM(source.VisitNumberCACPro),
 target.HospitalizationDate = source.HospitalizationDate,
 target.ProcedureDate = source.ProcedureDate,
 target.TransactionType = TRIM(source.TransactionType),
 target.CPTUnits = source.CPTUnits,
 target.PerformedByProviderId = source.PerformedByProviderId,
 target.OrderedByCode = source.OrderedByCode,
 target.ProcedureCode = TRIM(source.ProcedureCode),
 target.CPTModifier1 = TRIM(source.CPTModifier1),
 target.CPTModifier2 = TRIM(source.CPTModifier2),
 target.CPTModifier3 = TRIM(source.CPTModifier3),
 target.CPTModifier4 = TRIM(source.CPTModifier4),
 target.FileName = TRIM(source.FileName),
 target.FileDate = source.FileDate,
 target.FileImportedDate = source.FileImportedDate,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (CACProChargeKey, MessageDateTime, BatchDate, DocumentId, OperatorId, PatientSourceMRN, PatientLastName, PatientFirstName, PatientDateOfBirth, VisitNumber, AttendingID, AttendingName, ReferringId, ReferringName, HospitalService, VisitNumberCACPro, HospitalizationDate, ProcedureDate, TransactionType, CPTUnits, PerformedByProviderId, OrderedByCode, ProcedureCode, CPTModifier1, CPTModifier2, CPTModifier3, CPTModifier4, FileName, FileDate, FileImportedDate, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.CACProChargeKey, source.MessageDateTime, source.BatchDate, source.DocumentId, TRIM(source.OperatorId), TRIM(source.PatientSourceMRN), TRIM(source.PatientLastName), TRIM(source.PatientFirstName), source.PatientDateOfBirth, TRIM(source.VisitNumber), source.AttendingID, TRIM(source.AttendingName), source.ReferringId, TRIM(source.ReferringName), TRIM(source.HospitalService), TRIM(source.VisitNumberCACPro), source.HospitalizationDate, source.ProcedureDate, TRIM(source.TransactionType), source.CPTUnits, source.PerformedByProviderId, source.OrderedByCode, TRIM(source.ProcedureCode), TRIM(source.CPTModifier1), TRIM(source.CPTModifier2), TRIM(source.CPTModifier3), TRIM(source.CPTModifier4), TRIM(source.FileName), source.FileDate, source.FileImportedDate, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CACProChargeKey
      FROM {{ params.param_psc_core_dataset_name }}.OpenConnect_FactSourceCACProCharge
      GROUP BY CACProChargeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.OpenConnect_FactSourceCACProCharge');
ELSE
  COMMIT TRANSACTION;
END IF;
