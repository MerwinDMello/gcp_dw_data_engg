
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.Artiva_RprtPool AS target
USING {{ params.param_psc_stage_dataset_name }}.Artiva_RprtPool AS source
ON target.SnapShotDate = source.SnapShotDate
WHEN MATCHED THEN
  UPDATE SET
  target.SnapShotDate = source.SnapShotDate,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.TotalBalanceAmt = source.TotalBalanceAmt,
 target.ArtivaLiabilityPool = source.ArtivaLiabilityPool,
 target.PoolName = TRIM(source.PoolName),
 target.ArtivaLiabilityLastReviewedDate = source.ArtivaLiabilityLastReviewedDate,
 target.ArtivaLiabilityLastWorkedDate = source.ArtivaLiabilityLastWorkedDate,
 target.ArtivaLiabilityFollowupDate = source.ArtivaLiabilityFollowupDate,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (SnapShotDate, ClaimKey, ClaimNumber, TotalBalanceAmt, ArtivaLiabilityPool, PoolName, ArtivaLiabilityLastReviewedDate, ArtivaLiabilityLastWorkedDate, ArtivaLiabilityFollowupDate, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.SnapShotDate, source.ClaimKey, source.ClaimNumber, source.TotalBalanceAmt, source.ArtivaLiabilityPool, TRIM(source.PoolName), source.ArtivaLiabilityLastReviewedDate, source.ArtivaLiabilityLastWorkedDate, source.ArtivaLiabilityFollowupDate, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate
      FROM {{ params.param_psc_core_dataset_name }}.Artiva_RprtPool
      GROUP BY SnapShotDate
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Artiva_RprtPool');
ELSE
  COMMIT TRANSACTION;
END IF;
