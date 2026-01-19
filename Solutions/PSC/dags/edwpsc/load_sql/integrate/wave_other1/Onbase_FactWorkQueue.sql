
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.Onbase_FactWorkQueue AS target
USING {{ params.param_psc_stage_dataset_name }}.Onbase_FactWorkQueue AS source
ON target.OnbaseWorkQueueKey = source.OnbaseWorkQueueKey
WHEN MATCHED THEN
  UPDATE SET
  target.DocumentHandle = source.DocumentHandle,
 target.ItemName = TRIM(source.ItemName),
 target.LifeCycleName = TRIM(source.LifeCycleName),
 target.DocumentType = TRIM(source.DocumentType),
 target.QueueName = TRIM(source.QueueName),
 target.EntryDate = source.EntryDate,
 target.EntryTime = TRIM(source.EntryTime),
 target.ExitDate = source.ExitDate,
 target.ExitTime = TRIM(source.ExitTime),
 target.UserName = TRIM(source.UserName),
 target.UserId = TRIM(source.UserId),
 target.ProviderLastName = TRIM(source.ProviderLastName),
 target.ProviderFirstName = TRIM(source.ProviderFirstName),
 target.Color = TRIM(source.Color),
 target.NPI = TRIM(source.NPI),
 target.Decision = TRIM(source.Decision),
 target.Status = TRIM(source.Status),
 target.PacketType = TRIM(source.PacketType),
 target.EffectiveDate = TRIM(source.EffectiveDate),
 target.NumberOfDaysOpen = source.NumberOfDaysOpen,
 target.CompleteFlag = source.CompleteFlag,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.ARQueue = TRIM(source.ARQueue),
 target.Urgency = TRIM(source.Urgency),
 target.Payor = TRIM(source.Payor),
 target.StartDate = source.StartDate,
 target.DateStored = TRIM(source.DateStored),
 target.ItemStatus = source.ItemStatus,
 target.PatientName = TRIM(source.PatientName),
 target.WFScrub1 = TRIM(source.WFScrub1),
 target.WFScrub2 = TRIM(source.WFScrub2),
 target.Coid = TRIM(source.Coid),
 target.ERABatchNumber = source.ERABatchNumber,
 target.ClinicCode = TRIM(source.ClinicCode),
 target.TreasuryBatchNumber = TRIM(source.TreasuryBatchNumber),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.OnbaseWorkQueueKey = source.OnbaseWorkQueueKey
WHEN NOT MATCHED THEN
  INSERT (DocumentHandle, ItemName, LifeCycleName, DocumentType, QueueName, EntryDate, EntryTime, ExitDate, ExitTime, UserName, UserId, ProviderLastName, ProviderFirstName, Color, NPI, Decision, Status, PacketType, EffectiveDate, NumberOfDaysOpen, CompleteFlag, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ARQueue, Urgency, Payor, StartDate, DateStored, ItemStatus, PatientName, WFScrub1, WFScrub2, Coid, ERABatchNumber, ClinicCode, TreasuryBatchNumber, DWLastUpdateDateTime, OnbaseWorkQueueKey)
  VALUES (source.DocumentHandle, TRIM(source.ItemName), TRIM(source.LifeCycleName), TRIM(source.DocumentType), TRIM(source.QueueName), source.EntryDate, TRIM(source.EntryTime), source.ExitDate, TRIM(source.ExitTime), TRIM(source.UserName), TRIM(source.UserId), TRIM(source.ProviderLastName), TRIM(source.ProviderFirstName), TRIM(source.Color), TRIM(source.NPI), TRIM(source.Decision), TRIM(source.Status), TRIM(source.PacketType), TRIM(source.EffectiveDate), source.NumberOfDaysOpen, source.CompleteFlag, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.ARQueue), TRIM(source.Urgency), TRIM(source.Payor), source.StartDate, TRIM(source.DateStored), source.ItemStatus, TRIM(source.PatientName), TRIM(source.WFScrub1), TRIM(source.WFScrub2), TRIM(source.Coid), source.ERABatchNumber, TRIM(source.ClinicCode), TRIM(source.TreasuryBatchNumber), source.DWLastUpdateDateTime, source.OnbaseWorkQueueKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT OnbaseWorkQueueKey
      FROM {{ params.param_psc_core_dataset_name }}.Onbase_FactWorkQueue
      GROUP BY OnbaseWorkQueueKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Onbase_FactWorkQueue');
ELSE
  COMMIT TRANSACTION;
END IF;
