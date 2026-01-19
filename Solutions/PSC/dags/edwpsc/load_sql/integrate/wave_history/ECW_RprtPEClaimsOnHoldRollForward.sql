
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtPEClaimsOnHoldRollForward AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RprtPEClaimsOnHoldRollForward AS source
ON target.SnapShotDate = source.SnapShotDate
WHEN MATCHED THEN
  UPDATE SET
  target.COID = TRIM(source.COID),
 target.GroupName = TRIM(source.GroupName),
 target.DivisionName = TRIM(source.DivisionName),
 target.MarketName = TRIM(source.MarketName),
 target.LOB = TRIM(source.LOB),
 target.ProviderId = source.ProviderId,
 target.ProviderName = TRIM(source.ProviderName),
 target.FileStatus = TRIM(source.FileStatus),
 target.Amount = source.Amount,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.ColumnType = TRIM(source.ColumnType),
 target.ClaimCount = source.ClaimCount,
 target.SnapShotDate = source.SnapShotDate,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (COID, GroupName, DivisionName, MarketName, LOB, ProviderId, ProviderName, FileStatus, Amount, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ColumnType, ClaimCount, SnapShotDate, DWLastUpdateDateTime)
  VALUES (TRIM(source.COID), TRIM(source.GroupName), TRIM(source.DivisionName), TRIM(source.MarketName), TRIM(source.LOB), source.ProviderId, TRIM(source.ProviderName), TRIM(source.FileStatus), source.Amount, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.ColumnType), source.ClaimCount, source.SnapShotDate, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RprtPEClaimsOnHoldRollForward
      GROUP BY SnapShotDate
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RprtPEClaimsOnHoldRollForward');
ELSE
  COMMIT TRANSACTION;
END IF;
