
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_RefEPICWorkQueues AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_RefEPICWorkQueues AS source
ON target.WorkQueueKey = source.WorkQueueKey
WHEN MATCHED THEN
  UPDATE SET
  target.WorkQueueKey = source.WorkQueueKey,
 target.WorkQueueID = TRIM(source.WorkQueueID),
 target.CCUQueueFlag = source.CCUQueueFlag,
 target.PracticeQueueFlag = source.PracticeQueueFlag,
 target.AccountResolutionQueueFlag = source.AccountResolutionQueueFlag,
 target.VendorQueueFlag = source.VendorQueueFlag,
 target.CCUVendorQueueFlag = source.CCUVendorQueueFlag,
 target.Active = source.Active,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.RegionKey = source.RegionKey
WHEN NOT MATCHED THEN
  INSERT (WorkQueueKey, WorkQueueID, CCUQueueFlag, PracticeQueueFlag, AccountResolutionQueueFlag, VendorQueueFlag, CCUVendorQueueFlag, Active, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, RegionKey)
  VALUES (source.WorkQueueKey, TRIM(source.WorkQueueID), source.CCUQueueFlag, source.PracticeQueueFlag, source.AccountResolutionQueueFlag, source.VendorQueueFlag, source.CCUVendorQueueFlag, source.Active, source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.RegionKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT WorkQueueKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_RefEPICWorkQueues
      GROUP BY WorkQueueKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_RefEPICWorkQueues');
ELSE
  COMMIT TRANSACTION;
END IF;
