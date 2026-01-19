
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtICCReleasedClaims AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RprtICCReleasedClaims AS source
ON target.SnapShotDate = source.SnapShotDate AND target.ClaimKey = source.ClaimKey
WHEN MATCHED THEN
  UPDATE SET
  target.SnapShotDate = source.SnapShotDate,
 target.Coid = TRIM(source.Coid),
 target.ClaimNumber = source.ClaimNumber,
 target.RHClaimHistorySubmissionDateKey = TRIM(source.RHClaimHistorySubmissionDateKey),
 target.RHClaimHistoryBridgeFileNumber = TRIM(source.RHClaimHistoryBridgeFileNumber),
 target.RHClaimHistoryPatientControlNbr = TRIM(source.RHClaimHistoryPatientControlNbr),
 target.RHClaimHistoryHoldCode = TRIM(source.RHClaimHistoryHoldCode),
 target.RHClaimHistoryReleasedDateKey = source.RHClaimHistoryReleasedDateKey,
 target.RHClaimHistoryTotalClaimAmount = source.RHClaimHistoryTotalClaimAmount,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.ClaimKey = source.ClaimKey,
 target.RegionKey = source.RegionKey,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (SnapShotDate, Coid, ClaimNumber, RHClaimHistorySubmissionDateKey, RHClaimHistoryBridgeFileNumber, RHClaimHistoryPatientControlNbr, RHClaimHistoryHoldCode, RHClaimHistoryReleasedDateKey, RHClaimHistoryTotalClaimAmount, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ClaimKey, RegionKey, DWLastUpdateDateTime)
  VALUES (source.SnapShotDate, TRIM(source.Coid), source.ClaimNumber, TRIM(source.RHClaimHistorySubmissionDateKey), TRIM(source.RHClaimHistoryBridgeFileNumber), TRIM(source.RHClaimHistoryPatientControlNbr), TRIM(source.RHClaimHistoryHoldCode), source.RHClaimHistoryReleasedDateKey, source.RHClaimHistoryTotalClaimAmount, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.ClaimKey, source.RegionKey, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, ClaimKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RprtICCReleasedClaims
      GROUP BY SnapShotDate, ClaimKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RprtICCReleasedClaims');
ELSE
  COMMIT TRANSACTION;
END IF;
