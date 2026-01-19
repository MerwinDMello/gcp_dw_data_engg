
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_JuncFacilityResourceCoid AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_JuncFacilityResourceCoid AS source
ON target.JuncFacilityResourceCoidKey = source.JuncFacilityResourceCoidKey
WHEN MATCHED THEN
  UPDATE SET
  target.JuncFacilityResourceCoidKey = source.JuncFacilityResourceCoidKey,
 target.FacilityResourceKey = source.FacilityResourceKey,
 target.Coid = TRIM(source.Coid),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (JuncFacilityResourceCoidKey, FacilityResourceKey, Coid, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.JuncFacilityResourceCoidKey, source.FacilityResourceKey, TRIM(source.Coid), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT JuncFacilityResourceCoidKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_JuncFacilityResourceCoid
      GROUP BY JuncFacilityResourceCoidKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_JuncFacilityResourceCoid');
ELSE
  COMMIT TRANSACTION;
END IF;
