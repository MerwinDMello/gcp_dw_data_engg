
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.OpenConnect_FactSourceCernerMasterLog AS target
USING {{ params.param_psc_stage_dataset_name }}.OpenConnect_FactSourceCernerMasterLog AS source
ON target.CernerMasterLogKey = source.CernerMasterLogKey
WHEN MATCHED THEN
  UPDATE SET
  target.CernerMasterLogKey = source.CernerMasterLogKey,
 target.FacilityId = TRIM(source.FacilityId),
 target.Department = TRIM(source.Department),
 target.CDMCode = TRIM(source.CDMCode),
 target.CDMCodeName = TRIM(source.CDMCodeName),
 target.PatientNameFull = TRIM(source.PatientNameFull),
 target.PatientLastName = TRIM(source.PatientLastName),
 target.PatientFirstName = TRIM(source.PatientFirstName),
 target.VisitNumber = TRIM(source.VisitNumber),
 target.AccountType = TRIM(source.AccountType),
 target.BatchDate = source.BatchDate,
 target.BatchNumber = TRIM(source.BatchNumber),
 target.Entry = TRIM(source.Entry),
 target.ServiceDate = source.ServiceDate,
 target.PerformingProviderName = TRIM(source.PerformingProviderName),
 target.OrderingProviderName = TRIM(source.OrderingProviderName),
 target.PostDate = source.PostDate,
 target.CPTUnits = source.CPTUnits,
 target.TransactionAmt = source.TransactionAmt,
 target.ProcedureCode = TRIM(source.ProcedureCode),
 target.BillItemId = source.BillItemId,
 target.VisitId = TRIM(source.VisitId),
 target.ProcedureDateTime = source.ProcedureDateTime,
 target.ProcedureDate = source.ProcedureDate,
 target.EstArriveDateTime = source.EstArriveDateTime,
 target.DischDateTime = source.DischDateTime,
 target.FinancialClass = TRIM(source.FinancialClass),
 target.TierGroup = TRIM(source.TierGroup),
 target.MessageId = TRIM(source.MessageId),
 target.FileName = TRIM(source.FileName),
 target.FileDate = source.FileDate,
 target.FileImportedDate = source.FileImportedDate,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (CernerMasterLogKey, FacilityId, Department, CDMCode, CDMCodeName, PatientNameFull, PatientLastName, PatientFirstName, VisitNumber, AccountType, BatchDate, BatchNumber, Entry, ServiceDate, PerformingProviderName, OrderingProviderName, PostDate, CPTUnits, TransactionAmt, ProcedureCode, BillItemId, VisitId, ProcedureDateTime, ProcedureDate, EstArriveDateTime, DischDateTime, FinancialClass, TierGroup, MessageId, FileName, FileDate, FileImportedDate, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.CernerMasterLogKey, TRIM(source.FacilityId), TRIM(source.Department), TRIM(source.CDMCode), TRIM(source.CDMCodeName), TRIM(source.PatientNameFull), TRIM(source.PatientLastName), TRIM(source.PatientFirstName), TRIM(source.VisitNumber), TRIM(source.AccountType), source.BatchDate, TRIM(source.BatchNumber), TRIM(source.Entry), source.ServiceDate, TRIM(source.PerformingProviderName), TRIM(source.OrderingProviderName), source.PostDate, source.CPTUnits, source.TransactionAmt, TRIM(source.ProcedureCode), source.BillItemId, TRIM(source.VisitId), source.ProcedureDateTime, source.ProcedureDate, source.EstArriveDateTime, source.DischDateTime, TRIM(source.FinancialClass), TRIM(source.TierGroup), TRIM(source.MessageId), TRIM(source.FileName), source.FileDate, source.FileImportedDate, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CernerMasterLogKey
      FROM {{ params.param_psc_core_dataset_name }}.OpenConnect_FactSourceCernerMasterLog
      GROUP BY CernerMasterLogKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.OpenConnect_FactSourceCernerMasterLog');
ELSE
  COMMIT TRANSACTION;
END IF;
