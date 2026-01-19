
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactWorkQueuePreBillEditHistory AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactWorkQueuePreBillEditHistory AS source
ON target.PreBillEditHistoryKey = source.PreBillEditHistoryKey
WHEN MATCHED THEN
  UPDATE SET
  target.PreBillEditHistoryKey = source.PreBillEditHistoryKey,
 target.RegionKey = source.RegionKey,
 target.ClaimKey = source.ClaimKey,
 target.EncounterKey = source.EncounterKey,
 target.EncounterId = source.EncounterId,
 target.ActivityStartDate = source.ActivityStartDate,
 target.ActivityEndDate = source.ActivityEndDate,
 target.ActivityName = TRIM(source.ActivityName),
 target.WorkQueueId = TRIM(source.WorkQueueId),
 target.WorkQueueName = TRIM(source.WorkQueueName),
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceBPrimaryKeyValue = source.SourceBPrimaryKeyValue,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (PreBillEditHistoryKey, RegionKey, ClaimKey, EncounterKey, EncounterId, ActivityStartDate, ActivityEndDate, ActivityName, WorkQueueId, WorkQueueName, SourceAPrimaryKeyValue, SourceBPrimaryKeyValue, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.PreBillEditHistoryKey, source.RegionKey, source.ClaimKey, source.EncounterKey, source.EncounterId, source.ActivityStartDate, source.ActivityEndDate, TRIM(source.ActivityName), TRIM(source.WorkQueueId), TRIM(source.WorkQueueName), source.SourceAPrimaryKeyValue, source.SourceBPrimaryKeyValue, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PreBillEditHistoryKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactWorkQueuePreBillEditHistory
      GROUP BY PreBillEditHistoryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactWorkQueuePreBillEditHistory');
ELSE
  COMMIT TRANSACTION;
END IF;
