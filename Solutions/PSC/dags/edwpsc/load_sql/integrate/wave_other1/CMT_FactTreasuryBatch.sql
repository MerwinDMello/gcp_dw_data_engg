
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CMT_FactTreasuryBatch AS target
USING {{ params.param_psc_stage_dataset_name }}.CMT_FactTreasuryBatch AS source
ON target.TreasuryBatchKey = source.TreasuryBatchKey
WHEN MATCHED THEN
  UPDATE SET
  target.TreasuryBatchKey = source.TreasuryBatchKey,
 target.PaymentId = source.PaymentId,
 target.RegionId = source.RegionId,
 target.SourceSystem = TRIM(source.SourceSystem),
 target.BatchId = TRIM(source.BatchId),
 target.CreatedBy = TRIM(source.CreatedBy),
 target.BatchCreateDate = source.BatchCreateDate,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (TreasuryBatchKey, PaymentId, RegionId, SourceSystem, BatchId, CreatedBy, BatchCreateDate, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.TreasuryBatchKey, source.PaymentId, source.RegionId, TRIM(source.SourceSystem), TRIM(source.BatchId), TRIM(source.CreatedBy), source.BatchCreateDate, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT TreasuryBatchKey
      FROM {{ params.param_psc_core_dataset_name }}.CMT_FactTreasuryBatch
      GROUP BY TreasuryBatchKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CMT_FactTreasuryBatch');
ELSE
  COMMIT TRANSACTION;
END IF;
