
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotClaimUnBilledStatus AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactSnapShotClaimUnBilledStatus AS source
ON target.SnapShotDate = source.SnapShotDate AND target.ClaimKey = source.ClaimKey
WHEN MATCHED THEN
  UPDATE SET
  target.UnbilledStatusSnapShotKey = source.UnbilledStatusSnapShotKey,
 target.MonthID = source.MonthID,
 target.SnapShotDate = source.SnapShotDate,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.PatientInternalID = source.PatientInternalID,
 target.VisitNumber = source.VisitNumber,
 target.UnBilledStatusKey = source.UnBilledStatusKey,
 target.ClaimUnBilledStatusInRHInventory = source.ClaimUnBilledStatusInRHInventory,
 target.ClaimUnBilledStatusRHHoldCode = TRIM(source.ClaimUnBilledStatusRHHoldCode),
 target.ClaimUnBilledStatusEdiNoHold = source.ClaimUnBilledStatusEdiNoHold,
 target.ClaimUnBilledStatusMinSubmissionDate = source.ClaimUnBilledStatusMinSubmissionDate,
 target.ClaimUnBilledStatusMaxSubmissionDate = source.ClaimUnBilledStatusMaxSubmissionDate,
 target.ClaimUnBilledStatusClaimStatus = TRIM(source.ClaimUnBilledStatusClaimStatus),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM
WHEN NOT MATCHED THEN
  INSERT (UnbilledStatusSnapShotKey, MonthID, SnapShotDate, ClaimKey, ClaimNumber, RegionKey, Coid, PatientInternalID, VisitNumber, UnBilledStatusKey, ClaimUnBilledStatusInRHInventory, ClaimUnBilledStatusRHHoldCode, ClaimUnBilledStatusEdiNoHold, ClaimUnBilledStatusMinSubmissionDate, ClaimUnBilledStatusMaxSubmissionDate, ClaimUnBilledStatusClaimStatus, InsertedBy, InsertedDTM)
  VALUES (source.UnbilledStatusSnapShotKey, source.MonthID, source.SnapShotDate, source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.Coid), source.PatientInternalID, source.VisitNumber, source.UnBilledStatusKey, source.ClaimUnBilledStatusInRHInventory, TRIM(source.ClaimUnBilledStatusRHHoldCode), source.ClaimUnBilledStatusEdiNoHold, source.ClaimUnBilledStatusMinSubmissionDate, source.ClaimUnBilledStatusMaxSubmissionDate, TRIM(source.ClaimUnBilledStatusClaimStatus), TRIM(source.InsertedBy), source.InsertedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, ClaimKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotClaimUnBilledStatus
      GROUP BY SnapShotDate, ClaimKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotClaimUnBilledStatus');
ELSE
  COMMIT TRANSACTION;
END IF;
