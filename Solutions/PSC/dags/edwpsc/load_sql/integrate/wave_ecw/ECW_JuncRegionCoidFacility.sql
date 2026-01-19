
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncRegionCoidFacility AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_JuncRegionCoidFacility AS source
ON target.JuncRegionCoidFacilityKey = source.JuncRegionCoidFacilityKey
WHEN MATCHED THEN
  UPDATE SET
  target.JuncRegionCoidFacilityKey = source.JuncRegionCoidFacilityKey,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.FacilityKey = source.FacilityKey,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (JuncRegionCoidFacilityKey, RegionKey, Coid, FacilityKey, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.JuncRegionCoidFacilityKey, source.RegionKey, TRIM(source.Coid), source.FacilityKey, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT JuncRegionCoidFacilityKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_JuncRegionCoidFacility
      GROUP BY JuncRegionCoidFacilityKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_JuncRegionCoidFacility');
ELSE
  COMMIT TRANSACTION;
END IF;
