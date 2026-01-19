
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.Onbase_FactRemotePrintRequest AS target
USING {{ params.param_psc_stage_dataset_name }}.Onbase_FactRemotePrintRequest AS source
ON target.OnbaseRemotePrintRequestKey = source.OnbaseRemotePrintRequestKey
WHEN MATCHED THEN
  UPDATE SET
  target.DocumentHandle = source.DocumentHandle,
 target.QueueName = TRIM(source.QueueName),
 target.ItemName = TRIM(source.ItemName),
 target.LifeCycleName = TRIM(source.LifeCycleName),
 target.DocumentType = TRIM(source.DocumentType),
 target.EntryDate = TRIM(source.EntryDate),
 target.ExitDate = TRIM(source.ExitDate),
 target.UserName = TRIM(source.UserName),
 target.UserId = TRIM(source.UserId),
 target.Status = TRIM(source.Status),
 target.DateStored = TRIM(source.DateStored),
 target.ItemStatus = source.ItemStatus,
 target.PatientName = TRIM(source.PatientName),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.ServerName = TRIM(source.ServerName),
 target.Region = source.Region,
 target.ClinicCode = TRIM(source.ClinicCode),
 target.PatientAccountNumber = TRIM(source.PatientAccountNumber),
 target.WorkflowTransactionNumber = source.WorkflowTransactionNumber,
 target.WorkflowTransactionEntryDate = source.WorkflowTransactionEntryDate,
 target.WorkflowTransactionExitDate = source.WorkflowTransactionExitDate,
 target.WorkflowTransactionType = TRIM(source.WorkflowTransactionType),
 target.WorkflowTransactionName = TRIM(source.WorkflowTransactionName),
 target.WorkflowTransactionUserName = TRIM(source.WorkflowTransactionUserName),
 target.WorkflowTransactionUserId = TRIM(source.WorkflowTransactionUserId),
 target.OnbaseRemotePrintRequestKey = source.OnbaseRemotePrintRequestKey
WHEN NOT MATCHED THEN
  INSERT (DocumentHandle, QueueName, ItemName, LifeCycleName, DocumentType, EntryDate, ExitDate, UserName, UserId, Status, DateStored, ItemStatus, PatientName, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ServerName, Region, ClinicCode, PatientAccountNumber, WorkflowTransactionNumber, WorkflowTransactionEntryDate, WorkflowTransactionExitDate, WorkflowTransactionType, WorkflowTransactionName, WorkflowTransactionUserName, WorkflowTransactionUserId, OnbaseRemotePrintRequestKey)
  VALUES (source.DocumentHandle, TRIM(source.QueueName), TRIM(source.ItemName), TRIM(source.LifeCycleName), TRIM(source.DocumentType), TRIM(source.EntryDate), TRIM(source.ExitDate), TRIM(source.UserName), TRIM(source.UserId), TRIM(source.Status), TRIM(source.DateStored), source.ItemStatus, TRIM(source.PatientName), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.ServerName), source.Region, TRIM(source.ClinicCode), TRIM(source.PatientAccountNumber), source.WorkflowTransactionNumber, source.WorkflowTransactionEntryDate, source.WorkflowTransactionExitDate, TRIM(source.WorkflowTransactionType), TRIM(source.WorkflowTransactionName), TRIM(source.WorkflowTransactionUserName), TRIM(source.WorkflowTransactionUserId), source.OnbaseRemotePrintRequestKey);

-- DELETE FROM {{ params.param_psc_core_dataset_name }}.Onbase_FactRemotePrintRequest WHERE DATE(InsertedDTM) < DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR);


SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT OnbaseRemotePrintRequestKey
      FROM {{ params.param_psc_core_dataset_name }}.Onbase_FactRemotePrintRequest
      GROUP BY OnbaseRemotePrintRequestKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Onbase_FactRemotePrintRequest');
ELSE
  COMMIT TRANSACTION;
END IF;
