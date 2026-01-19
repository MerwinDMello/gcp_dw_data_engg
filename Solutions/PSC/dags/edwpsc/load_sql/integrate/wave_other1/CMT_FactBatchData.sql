
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CMT_FactBatchData AS target
USING {{ params.param_psc_stage_dataset_name }}.CMT_FactBatchData AS source
ON target.BatchDataKey = source.BatchDataKey
WHEN MATCHED THEN
  UPDATE SET
  target.BatchDataKey = source.BatchDataKey,
 target.BatchId = TRIM(source.BatchId),
 target.UserId = TRIM(source.UserId),
 target.UpdateDate = source.UpdateDate,
 target.HistoryReasonId = source.HistoryReasonId,
 target.HistoryReasonCode = TRIM(source.HistoryReasonCode),
 target.BatchState = source.BatchState,
 target.BatchStateName = TRIM(source.BatchStateName),
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.PayerName = TRIM(source.PayerName),
 target.CheckNumber = TRIM(source.CheckNumber),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (BatchDataKey, BatchId, UserId, UpdateDate, HistoryReasonId, HistoryReasonCode, BatchState, BatchStateName, SourceAPrimaryKeyValue, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, PayerName, CheckNumber, DWLastUpdateDateTime)
  VALUES (source.BatchDataKey, TRIM(source.BatchId), TRIM(source.UserId), source.UpdateDate, source.HistoryReasonId, TRIM(source.HistoryReasonCode), source.BatchState, TRIM(source.BatchStateName), source.SourceAPrimaryKeyValue, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.PayerName), TRIM(source.CheckNumber), source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT BatchDataKey
      FROM {{ params.param_psc_core_dataset_name }}.CMT_FactBatchData
      GROUP BY BatchDataKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CMT_FactBatchData');
ELSE
  COMMIT TRANSACTION;
END IF;
