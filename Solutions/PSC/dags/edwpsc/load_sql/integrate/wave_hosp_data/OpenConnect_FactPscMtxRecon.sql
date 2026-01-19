
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.OpenConnect_FactPscMtxRecon AS target
USING {{ params.param_psc_stage_dataset_name }}.OpenConnect_FactPscMtxRecon AS source
ON target.OpenConnectPscMtxReconKey = source.OpenConnectPscMtxReconKey
WHEN MATCHED THEN
  UPDATE SET
  target.OpenConnectPscMtxReconKey = source.OpenConnectPscMtxReconKey,
 target.MessageControlId = TRIM(source.MessageControlId),
 target.BatchDate = source.BatchDate,
 target.VisitNumber = TRIM(source.VisitNumber),
 target.PatientAccountNumber = TRIM(source.PatientAccountNumber),
 target.ProcedureCode = TRIM(source.ProcedureCode),
 target.CPTCode = TRIM(source.CPTCode),
 target.ServiceDate = source.ServiceDate,
 target.CPTUnits = source.CPTUnits,
 target.FacilityLocationId = TRIM(source.FacilityLocationId),
 target.Coid = TRIM(source.Coid),
 target.FileDate = source.FileDate,
 target.FileName = TRIM(source.FileName),
 target.FileImportedDate = source.FileImportedDate,
 target.SourceFileType = TRIM(source.SourceFileType),
 target.SendingMTX = TRIM(source.SendingMTX),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (OpenConnectPscMtxReconKey, MessageControlId, BatchDate, VisitNumber, PatientAccountNumber, ProcedureCode, CPTCode, ServiceDate, CPTUnits, FacilityLocationId, Coid, FileDate, FileName, FileImportedDate, SourceFileType, SendingMTX, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.OpenConnectPscMtxReconKey, TRIM(source.MessageControlId), source.BatchDate, TRIM(source.VisitNumber), TRIM(source.PatientAccountNumber), TRIM(source.ProcedureCode), TRIM(source.CPTCode), source.ServiceDate, source.CPTUnits, TRIM(source.FacilityLocationId), TRIM(source.Coid), source.FileDate, TRIM(source.FileName), source.FileImportedDate, TRIM(source.SourceFileType), TRIM(source.SendingMTX), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT OpenConnectPscMtxReconKey
      FROM {{ params.param_psc_core_dataset_name }}.OpenConnect_FactPscMtxRecon
      GROUP BY OpenConnectPscMtxReconKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.OpenConnect_FactPscMtxRecon');
ELSE
  COMMIT TRANSACTION;
END IF;
