
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncRegionCoidProvider AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_JuncRegionCoidProvider AS source
ON target.JuncRegionCoidProviderKey = source.JuncRegionCoidProviderKey
WHEN MATCHED THEN
  UPDATE SET
  target.JuncRegionCoidProviderKey = source.JuncRegionCoidProviderKey,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.ProviderKey = source.ProviderKey,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (JuncRegionCoidProviderKey, RegionKey, Coid, ProviderKey, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.JuncRegionCoidProviderKey, source.RegionKey, TRIM(source.Coid), source.ProviderKey, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT JuncRegionCoidProviderKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_JuncRegionCoidProvider
      GROUP BY JuncRegionCoidProviderKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_JuncRegionCoidProvider');
ELSE
  COMMIT TRANSACTION;
END IF;
