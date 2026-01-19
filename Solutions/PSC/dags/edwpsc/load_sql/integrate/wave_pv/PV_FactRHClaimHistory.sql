
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactRHClaimHistory AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactRHClaimHistory AS source
ON target.RHClaimHistoryKey = source.RHClaimHistoryKey
WHEN MATCHED THEN
  UPDATE SET
  target.RHClaimHistoryKey = source.RHClaimHistoryKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.Coid = TRIM(source.Coid),
 target.RegionKey = source.RegionKey,
 target.ImportDateKey = source.ImportDateKey,
 target.RHClaimHistoryClientId = TRIM(source.RHClaimHistoryClientId),
 target.RHClaimHistorySubmissionDateKey = source.RHClaimHistorySubmissionDateKey,
 target.RHClaimHistoryBridgeFileNumber = TRIM(source.RHClaimHistoryBridgeFileNumber),
 target.RHClaimHistoryClaimID = TRIM(source.RHClaimHistoryClaimID),
 target.RHClaimHistoryPatientControlNbr = TRIM(source.RHClaimHistoryPatientControlNbr),
 target.RHClaimHistoryOriginalClaimStatusCode = TRIM(source.RHClaimHistoryOriginalClaimStatusCode),
 target.RHClaimHistoryPayerIndicator = TRIM(source.RHClaimHistoryPayerIndicator),
 target.RHClaimHistoryOriginalClaimAmount = TRIM(source.RHClaimHistoryOriginalClaimAmount),
 target.RHClaimHistoryOriginalErrorCount = TRIM(source.RHClaimHistoryOriginalErrorCount),
 target.RHClaimHistoryReleasedDateKey = source.RHClaimHistoryReleasedDateKey,
 target.RHClaimHistoryClaimState = TRIM(source.RHClaimHistoryClaimState),
 target.RHClaimHistoryHoldCode = TRIM(source.RHClaimHistoryHoldCode),
 target.RHClaimHistoryCurrentClaimStatusCode = TRIM(source.RHClaimHistoryCurrentClaimStatusCode),
 target.RHClaimHistoryErrorCount = TRIM(source.RHClaimHistoryErrorCount),
 target.RHClaimHistoryTotalClaimAmount = TRIM(source.RHClaimHistoryTotalClaimAmount),
 target.RHClaimHistoryFileName = TRIM(source.RHClaimHistoryFileName),
 target.SourcePrimaryKeyValue = TRIM(source.SourcePrimaryKeyValue),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (RHClaimHistoryKey, ClaimKey, ClaimNumber, Coid, RegionKey, ImportDateKey, RHClaimHistoryClientId, RHClaimHistorySubmissionDateKey, RHClaimHistoryBridgeFileNumber, RHClaimHistoryClaimID, RHClaimHistoryPatientControlNbr, RHClaimHistoryOriginalClaimStatusCode, RHClaimHistoryPayerIndicator, RHClaimHistoryOriginalClaimAmount, RHClaimHistoryOriginalErrorCount, RHClaimHistoryReleasedDateKey, RHClaimHistoryClaimState, RHClaimHistoryHoldCode, RHClaimHistoryCurrentClaimStatusCode, RHClaimHistoryErrorCount, RHClaimHistoryTotalClaimAmount, RHClaimHistoryFileName, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.RHClaimHistoryKey, source.ClaimKey, source.ClaimNumber, TRIM(source.Coid), source.RegionKey, source.ImportDateKey, TRIM(source.RHClaimHistoryClientId), source.RHClaimHistorySubmissionDateKey, TRIM(source.RHClaimHistoryBridgeFileNumber), TRIM(source.RHClaimHistoryClaimID), TRIM(source.RHClaimHistoryPatientControlNbr), TRIM(source.RHClaimHistoryOriginalClaimStatusCode), TRIM(source.RHClaimHistoryPayerIndicator), TRIM(source.RHClaimHistoryOriginalClaimAmount), TRIM(source.RHClaimHistoryOriginalErrorCount), source.RHClaimHistoryReleasedDateKey, TRIM(source.RHClaimHistoryClaimState), TRIM(source.RHClaimHistoryHoldCode), TRIM(source.RHClaimHistoryCurrentClaimStatusCode), TRIM(source.RHClaimHistoryErrorCount), TRIM(source.RHClaimHistoryTotalClaimAmount), TRIM(source.RHClaimHistoryFileName), TRIM(source.SourcePrimaryKeyValue), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT RHClaimHistoryKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactRHClaimHistory
      GROUP BY RHClaimHistoryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactRHClaimHistory');
ELSE
  COMMIT TRANSACTION;
END IF;
