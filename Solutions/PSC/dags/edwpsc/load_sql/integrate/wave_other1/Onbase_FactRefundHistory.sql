
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.Onbase_FactRefundHistory AS target
USING {{ params.param_psc_stage_dataset_name }}.Onbase_FactRefundHistory AS source
ON target.OnbaseRefundHistoryKey = source.OnbaseRefundHistoryKey
WHEN MATCHED THEN
  UPDATE SET
  target.DocumentHandle = source.DocumentHandle,
 target.Status = TRIM(source.Status),
 target.ItemName = TRIM(source.ItemName),
 target.LifeCycleName = TRIM(source.LifeCycleName),
 target.QueueName = TRIM(source.QueueName),
 target.EntryTime = TRIM(source.EntryTime),
 target.ExitDate = TRIM(source.ExitDate),
 target.ExitTime = TRIM(source.ExitTime),
 target.DaysInQueue = TRIM(source.DaysInQueue),
 target.UserName = TRIM(source.UserName),
 target.HCA34_ID = TRIM(source.HCA34_ID),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.OnbaseRefundHistoryKey = source.OnbaseRefundHistoryKey
WHEN NOT MATCHED THEN
  INSERT (DocumentHandle, Status, ItemName, LifeCycleName, QueueName, EntryTime, ExitDate, ExitTime, DaysInQueue, UserName, HCA34_ID, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime, OnbaseRefundHistoryKey)
  VALUES (source.DocumentHandle, TRIM(source.Status), TRIM(source.ItemName), TRIM(source.LifeCycleName), TRIM(source.QueueName), TRIM(source.EntryTime), TRIM(source.ExitDate), TRIM(source.ExitTime), TRIM(source.DaysInQueue), TRIM(source.UserName), TRIM(source.HCA34_ID), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime, source.OnbaseRefundHistoryKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT OnbaseRefundHistoryKey
      FROM {{ params.param_psc_core_dataset_name }}.Onbase_FactRefundHistory
      GROUP BY OnbaseRefundHistoryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Onbase_FactRefundHistory');
ELSE
  COMMIT TRANSACTION;
END IF;
