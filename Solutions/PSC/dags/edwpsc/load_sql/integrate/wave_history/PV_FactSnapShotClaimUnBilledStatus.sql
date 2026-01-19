
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotClaimUnBilledStatus AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactSnapShotClaimUnBilledStatus AS source
ON target.SnapShotDate = source.SnapShotDate AND target.ClaimKey = source.ClaimKey
WHEN MATCHED THEN
  UPDATE SET
  target.UnbilledStatusSnapShotKey = source.UnbilledStatusSnapShotKey,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.Practice = TRIM(source.Practice),
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.UnbilledStatusKey = source.UnbilledStatusKey,
 target.ClaimUnbilledStatusInRHInventory = source.ClaimUnbilledStatusInRHInventory,
 target.ClaimUnbilledStatusRHHoldCode = TRIM(source.ClaimUnbilledStatusRHHoldCode),
 target.ClaimUnbilledStatusEdiNoHold = source.ClaimUnbilledStatusEdiNoHold,
 target.ClaimUnbilledStatusMinSubmissionDate = source.ClaimUnbilledStatusMinSubmissionDate,
 target.ClaimUnbilledStatusMaxSubmissionDate = source.ClaimUnbilledStatusMaxSubmissionDate,
 target.ClaimUnbilledStatusClaimStatus = TRIM(source.ClaimUnbilledStatusClaimStatus),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.SnapShotDate = source.SnapShotDate,
 target.RhUnbilledCategory = TRIM(source.RhUnbilledCategory),
 target.ClaimStatusOwner = TRIM(source.ClaimStatusOwner),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (UnbilledStatusSnapShotKey, RegionKey, Coid, Practice, ClaimKey, ClaimNumber, UnbilledStatusKey, ClaimUnbilledStatusInRHInventory, ClaimUnbilledStatusRHHoldCode, ClaimUnbilledStatusEdiNoHold, ClaimUnbilledStatusMinSubmissionDate, ClaimUnbilledStatusMaxSubmissionDate, ClaimUnbilledStatusClaimStatus, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, SnapShotDate, RhUnbilledCategory, ClaimStatusOwner, DWLastUpdateDateTime)
  VALUES (source.UnbilledStatusSnapShotKey, source.RegionKey, TRIM(source.Coid), TRIM(source.Practice), source.ClaimKey, source.ClaimNumber, source.UnbilledStatusKey, source.ClaimUnbilledStatusInRHInventory, TRIM(source.ClaimUnbilledStatusRHHoldCode), source.ClaimUnbilledStatusEdiNoHold, source.ClaimUnbilledStatusMinSubmissionDate, source.ClaimUnbilledStatusMaxSubmissionDate, TRIM(source.ClaimUnbilledStatusClaimStatus), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.SnapShotDate, TRIM(source.RhUnbilledCategory), TRIM(source.ClaimStatusOwner), source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, ClaimKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotClaimUnBilledStatus
      GROUP BY SnapShotDate, ClaimKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotClaimUnBilledStatus');
ELSE
  COMMIT TRANSACTION;
END IF;
