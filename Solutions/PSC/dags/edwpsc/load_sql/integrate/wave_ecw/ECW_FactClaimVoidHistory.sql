
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactClaimVoidHistory AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactClaimVoidHistory AS source
ON target.ClaimVoidHistoryKey = source.ClaimVoidHistoryKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimVoidHistoryKey = source.ClaimVoidHistoryKey,
 target.OldClaimNumber = source.OldClaimNumber,
 target.OldClaimKey = source.OldClaimKey,
 target.ReversedClaimNumber = source.ReversedClaimNumber,
 target.ReversedClaimKey = source.ReversedClaimKey,
 target.NewClaimNumber = source.NewClaimNumber,
 target.NewClaimKey = source.NewClaimKey,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DeleteFlag = source.DeleteFlag,
 target.RegionKey = source.RegionKey,
 target.COID = TRIM(source.COID),
 target.ArchivedRecord = TRIM(source.ArchivedRecord)
WHEN NOT MATCHED THEN
  INSERT (ClaimVoidHistoryKey, OldClaimNumber, OldClaimKey, ReversedClaimNumber, ReversedClaimKey, NewClaimNumber, NewClaimKey, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag, RegionKey, COID, ArchivedRecord)
  VALUES (source.ClaimVoidHistoryKey, source.OldClaimNumber, source.OldClaimKey, source.ReversedClaimNumber, source.ReversedClaimKey, source.NewClaimNumber, source.NewClaimKey, source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag, source.RegionKey, TRIM(source.COID), TRIM(source.ArchivedRecord));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimVoidHistoryKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactClaimVoidHistory
      GROUP BY ClaimVoidHistoryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactClaimVoidHistory');
ELSE
  COMMIT TRANSACTION;
END IF;
