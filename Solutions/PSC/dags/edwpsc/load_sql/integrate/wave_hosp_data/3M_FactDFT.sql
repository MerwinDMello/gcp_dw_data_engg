
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.3M_FactDFT AS target
USING {{ params.param_psc_stage_dataset_name }}.3M_FactDFT AS source
ON target.Dft3MKey = source.Dft3MKey
WHEN MATCHED THEN
  UPDATE SET
  target.Dft3MKey = source.Dft3MKey,
 target.BatchDateKey = source.BatchDateKey,
 target.BatchDateTime = source.BatchDateTime,
 target.VisitNumber = TRIM(source.VisitNumber),
 target.PatientAccountNumber = TRIM(source.PatientAccountNumber),
 target.ProcedureCode = TRIM(source.ProcedureCode),
 target.ProcedureType = TRIM(source.ProcedureType),
 target.ServiceDate = source.ServiceDate,
 target.CPTUnit = source.CPTUnit,
 target.FacilityId = TRIM(source.FacilityId),
 target.SenderNote = TRIM(source.SenderNote),
 target.FileName = TRIM(source.FileName),
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (Dft3MKey, BatchDateKey, BatchDateTime, VisitNumber, PatientAccountNumber, ProcedureCode, ProcedureType, ServiceDate, CPTUnit, FacilityId, SenderNote, FileName, SourceAPrimaryKeyValue, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.Dft3MKey, source.BatchDateKey, source.BatchDateTime, TRIM(source.VisitNumber), TRIM(source.PatientAccountNumber), TRIM(source.ProcedureCode), TRIM(source.ProcedureType), source.ServiceDate, source.CPTUnit, TRIM(source.FacilityId), TRIM(source.SenderNote), TRIM(source.FileName), TRIM(source.SourceAPrimaryKeyValue), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT Dft3MKey
      FROM {{ params.param_psc_core_dataset_name }}.3M_FactDFT
      GROUP BY Dft3MKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.3M_FactDFT');
ELSE
  COMMIT TRANSACTION;
END IF;
