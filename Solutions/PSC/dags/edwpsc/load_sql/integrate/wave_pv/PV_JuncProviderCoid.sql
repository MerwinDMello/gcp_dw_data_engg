
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_JuncProviderCoid AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_JuncProviderCoid AS source
ON target.JuncProviderCoid = source.JuncProviderCoid
WHEN MATCHED THEN
  UPDATE SET
  target.JuncProviderCoid = source.JuncProviderCoid,
 target.ProviderKey = source.ProviderKey,
 target.Coid = TRIM(source.Coid),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.ProviderType = source.ProviderType,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (JuncProviderCoid, ProviderKey, Coid, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ProviderType, DWLastUpdateDateTime)
  VALUES (source.JuncProviderCoid, source.ProviderKey, TRIM(source.Coid), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.ProviderType, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT JuncProviderCoid
      FROM {{ params.param_psc_core_dataset_name }}.PV_JuncProviderCoid
      GROUP BY JuncProviderCoid
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_JuncProviderCoid');
ELSE
  COMMIT TRANSACTION;
END IF;
