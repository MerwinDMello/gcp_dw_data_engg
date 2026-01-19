
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimStatusHistory AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactClaimStatusHistory AS source
ON target.ClaimStatusHistoryKey = source.ClaimStatusHistoryKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimStatusHistoryKey = source.ClaimStatusHistoryKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.VisitNumber = source.VisitNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.InvoiceNumber = TRIM(source.InvoiceNumber),
 target.ClaimStatusHistoryMessage = TRIM(source.ClaimStatusHistoryMessage),
 target.ClaimStatusHistoryFrom = TRIM(source.ClaimStatusHistoryFrom),
 target.ClaimStatusHistoryTo = TRIM(source.ClaimStatusHistoryTo),
 target.ClaimStatusHistoryChangeDate = source.ClaimStatusHistoryChangeDate,
 target.ClaimStatusHistoryChangeTime = source.ClaimStatusHistoryChangeTime,
 target.ClaimStatusToKey = source.ClaimStatusToKey,
 target.ClaimStatusHistoryChangedByUserKey = source.ClaimStatusHistoryChangedByUserKey,
 target.ClaimErrorMessage = TRIM(source.ClaimErrorMessage),
 target.ClaimStatusNote = TRIM(source.ClaimStatusNote),
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBPrimaryKeyValue = TRIM(source.SourceBPrimaryKeyValue),
 target.SourceBRecordLastUpdated = source.SourceBRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ClaimStatusHistoryKey, ClaimKey, ClaimNumber, VisitNumber, RegionKey, Coid, InvoiceNumber, ClaimStatusHistoryMessage, ClaimStatusHistoryFrom, ClaimStatusHistoryTo, ClaimStatusHistoryChangeDate, ClaimStatusHistoryChangeTime, ClaimStatusToKey, ClaimStatusHistoryChangedByUserKey, ClaimErrorMessage, ClaimStatusNote, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBPrimaryKeyValue, SourceBRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ClaimStatusHistoryKey, source.ClaimKey, source.ClaimNumber, source.VisitNumber, source.RegionKey, TRIM(source.Coid), TRIM(source.InvoiceNumber), TRIM(source.ClaimStatusHistoryMessage), TRIM(source.ClaimStatusHistoryFrom), TRIM(source.ClaimStatusHistoryTo), source.ClaimStatusHistoryChangeDate, source.ClaimStatusHistoryChangeTime, source.ClaimStatusToKey, source.ClaimStatusHistoryChangedByUserKey, TRIM(source.ClaimErrorMessage), TRIM(source.ClaimStatusNote), TRIM(source.SourceAPrimaryKeyValue), source.SourceARecordLastUpdated, TRIM(source.SourceBPrimaryKeyValue), source.SourceBRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimStatusHistoryKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimStatusHistory
      GROUP BY ClaimStatusHistoryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimStatusHistory');
ELSE
  COMMIT TRANSACTION;
END IF;
