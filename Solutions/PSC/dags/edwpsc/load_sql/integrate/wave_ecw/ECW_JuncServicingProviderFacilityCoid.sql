
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncServicingProviderFacilityCoid AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_JuncServicingProviderFacilityCoid AS source
ON target.JuncServicingProviderFacilityCoidKey = source.JuncServicingProviderFacilityCoidKey
WHEN MATCHED THEN
  UPDATE SET
  target.JuncServicingProviderFacilityCoidKey = source.JuncServicingProviderFacilityCoidKey,
 target.ProviderKey = source.ProviderKey,
 target.FacilityKey = source.FacilityKey,
 target.Coid = TRIM(source.Coid),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (JuncServicingProviderFacilityCoidKey, ProviderKey, FacilityKey, Coid, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.JuncServicingProviderFacilityCoidKey, source.ProviderKey, source.FacilityKey, TRIM(source.Coid), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT JuncServicingProviderFacilityCoidKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_JuncServicingProviderFacilityCoid
      GROUP BY JuncServicingProviderFacilityCoidKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_JuncServicingProviderFacilityCoid');
ELSE
  COMMIT TRANSACTION;
END IF;
