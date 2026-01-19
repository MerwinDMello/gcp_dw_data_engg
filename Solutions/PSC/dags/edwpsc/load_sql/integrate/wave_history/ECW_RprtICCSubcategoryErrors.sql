
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtICCSubcategoryErrors AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RprtICCSubcategoryErrors AS source
ON target.SnapShotDate = source.SnapShotDate AND target.ClaimKey = source.ClaimKey
WHEN MATCHED THEN
  UPDATE SET
  target.SnapshotDate = source.SnapshotDate,
 target.COID = TRIM(source.COID),
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.SubcategoryDescription = TRIM(source.SubcategoryDescription),
 target.ErrorCount = source.ErrorCount,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (SnapshotDate, COID, ClaimKey, ClaimNumber, SubcategoryDescription, ErrorCount, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.SnapshotDate, TRIM(source.COID), source.ClaimKey, source.ClaimNumber, TRIM(source.SubcategoryDescription), source.ErrorCount, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, ClaimKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RprtICCSubcategoryErrors
      GROUP BY SnapShotDate, ClaimKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RprtICCSubcategoryErrors');
ELSE
  COMMIT TRANSACTION;
END IF;
