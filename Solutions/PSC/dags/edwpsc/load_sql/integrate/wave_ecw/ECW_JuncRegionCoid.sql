
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncRegionCoid AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_JuncRegionCoid AS source
ON target.JuncRegionCoidKey = source.JuncRegionCoidKey
WHEN MATCHED THEN
  UPDATE SET
  target.JuncRegionCoidKey = source.JuncRegionCoidKey,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (JuncRegionCoidKey, RegionKey, Coid, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.JuncRegionCoidKey, source.RegionKey, TRIM(source.Coid), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT JuncRegionCoidKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_JuncRegionCoid
      GROUP BY JuncRegionCoidKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_JuncRegionCoid');
ELSE
  COMMIT TRANSACTION;
END IF;
