
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotClaimUnBilledStatus AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactSnapShotClaimUnBilledStatus AS source
ON target.snapshotdate = source.snapshotdate AND target.claimkey = source.claimkey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimKey = source.ClaimKey,
 target.MonthId = source.MonthId,
 target.SnapShotDate = source.SnapShotDate,
 target.ClaimNumber = source.ClaimNumber,
 target.UnBilledStatusKey = source.UnBilledStatusKey,
 target.ClaimUnBilledStatusInRHInventory = source.ClaimUnBilledStatusInRHInventory,
 target.ClaimUnBilledStatusRHHoldCode = TRIM(source.ClaimUnBilledStatusRHHoldCode),
 target.ClaimUnBilledStatusEdiNoHold = source.ClaimUnBilledStatusEdiNoHold,
 target.ClaimUnBilledStatusMinSubmissionDate = source.ClaimUnBilledStatusMinSubmissionDate,
 target.ClaimUnBilledStatusMaxSubmissionDate = source.ClaimUnBilledStatusMaxSubmissionDate,
 target.ClaimUnBilledStatusClaimStatus = TRIM(source.ClaimUnBilledStatusClaimStatus),
 target.Coid = TRIM(source.Coid),
 target.RhUnbilledCategory = TRIM(source.RhUnbilledCategory),
 target.HoldCategory = TRIM(source.HoldCategory),
 target.ClaimStatusOwner = TRIM(source.ClaimStatusOwner)
WHEN NOT MATCHED THEN
  INSERT (ClaimKey, MonthId, SnapShotDate, ClaimNumber, UnBilledStatusKey, ClaimUnBilledStatusInRHInventory, ClaimUnBilledStatusRHHoldCode, ClaimUnBilledStatusEdiNoHold, ClaimUnBilledStatusMinSubmissionDate, ClaimUnBilledStatusMaxSubmissionDate, ClaimUnBilledStatusClaimStatus, Coid, RhUnbilledCategory, HoldCategory, ClaimStatusOwner)
  VALUES (source.ClaimKey, source.MonthId, source.SnapShotDate, source.ClaimNumber, source.UnBilledStatusKey, source.ClaimUnBilledStatusInRHInventory, TRIM(source.ClaimUnBilledStatusRHHoldCode), source.ClaimUnBilledStatusEdiNoHold, source.ClaimUnBilledStatusMinSubmissionDate, source.ClaimUnBilledStatusMaxSubmissionDate, TRIM(source.ClaimUnBilledStatusClaimStatus), TRIM(source.Coid), TRIM(source.RhUnbilledCategory), TRIM(source.HoldCategory), TRIM(source.ClaimStatusOwner));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT snapshotdate, claimkey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotClaimUnBilledStatus
      GROUP BY snapshotdate, claimkey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotClaimUnBilledStatus');
ELSE
  COMMIT TRANSACTION;
END IF;
