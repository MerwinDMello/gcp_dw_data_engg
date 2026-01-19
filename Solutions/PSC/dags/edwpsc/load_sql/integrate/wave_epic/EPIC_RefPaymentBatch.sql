
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefPaymentBatch AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefPaymentBatch AS source
ON target.BatchKey = source.BatchKey
WHEN MATCHED THEN
  UPDATE SET
  target.BatchKey = source.BatchKey,
 target.BatchName = TRIM(source.BatchName),
 target.BatchDateKey = source.BatchDateKey,
 target.BatchOpenDate = source.BatchOpenDate,
 target.BatchClosedDate = source.BatchClosedDate,
 target.BatchTotalAmt = source.BatchTotalAmt,
 target.BatchPostedAmt = source.BatchPostedAmt,
 target.BatchStatus = source.BatchStatus,
 target.RemitFlag = source.RemitFlag,
 target.BatchTypeDesc = TRIM(source.BatchTypeDesc),
 target.BatchNote = TRIM(source.BatchNote),
 target.CreatedByUserKey = source.CreatedByUserKey,
 target.ClosedByUserKey = source.ClosedByUserKey,
 target.RegionKey = source.RegionKey,
 target.DeleteFlag = source.DeleteFlag,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (BatchKey, BatchName, BatchDateKey, BatchOpenDate, BatchClosedDate, BatchTotalAmt, BatchPostedAmt, BatchStatus, RemitFlag, BatchTypeDesc, BatchNote, CreatedByUserKey, ClosedByUserKey, RegionKey, DeleteFlag, SourceAPrimaryKeyValue, SourceARecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.BatchKey, TRIM(source.BatchName), source.BatchDateKey, source.BatchOpenDate, source.BatchClosedDate, source.BatchTotalAmt, source.BatchPostedAmt, source.BatchStatus, source.RemitFlag, TRIM(source.BatchTypeDesc), TRIM(source.BatchNote), source.CreatedByUserKey, source.ClosedByUserKey, source.RegionKey, source.DeleteFlag, source.SourceAPrimaryKeyValue, source.SourceARecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT BatchKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefPaymentBatch
      GROUP BY BatchKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefPaymentBatch');
ELSE
  COMMIT TRANSACTION;
END IF;
