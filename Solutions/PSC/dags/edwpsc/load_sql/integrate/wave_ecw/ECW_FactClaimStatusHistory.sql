
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactClaimStatusHistory AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactClaimStatusHistory AS source
ON target.ClaimStatusHistoryKey = source.ClaimStatusHistoryKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimStatusHistoryKey = source.ClaimStatusHistoryKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.ClaimStatusHistoryMessage = TRIM(source.ClaimStatusHistoryMessage),
 target.ClaimStatusHistoryFrom = TRIM(source.ClaimStatusHistoryFrom),
 target.ClaimStatusHistoryTo = TRIM(source.ClaimStatusHistoryTo),
 target.ClaimStatusHistoryChangeDate = source.ClaimStatusHistoryChangeDate,
 target.ClaimStatusHistoryChangeTime = source.ClaimStatusHistoryChangeTime,
 target.ClaimStatusHistoryChangedByUserKey = source.ClaimStatusHistoryChangedByUserKey,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.ArchivedRecord = TRIM(source.ArchivedRecord)
WHEN NOT MATCHED THEN
  INSERT (ClaimStatusHistoryKey, ClaimKey, ClaimNumber, RegionKey, Coid, ClaimStatusHistoryMessage, ClaimStatusHistoryFrom, ClaimStatusHistoryTo, ClaimStatusHistoryChangeDate, ClaimStatusHistoryChangeTime, ClaimStatusHistoryChangedByUserKey, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ArchivedRecord)
  VALUES (source.ClaimStatusHistoryKey, source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.Coid), TRIM(source.ClaimStatusHistoryMessage), TRIM(source.ClaimStatusHistoryFrom), TRIM(source.ClaimStatusHistoryTo), source.ClaimStatusHistoryChangeDate, source.ClaimStatusHistoryChangeTime, source.ClaimStatusHistoryChangedByUserKey, source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.ArchivedRecord));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimStatusHistoryKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactClaimStatusHistory
      GROUP BY ClaimStatusHistoryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactClaimStatusHistory');
ELSE
  COMMIT TRANSACTION;
END IF;
