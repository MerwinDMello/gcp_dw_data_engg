
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_JuncFactClaimToPayer AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_JuncFactClaimToPayer AS source
ON target.ClaimKey = source.ClaimKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimKey = source.ClaimKey,
 target.PrimaryClaimPayerKey = source.PrimaryClaimPayerKey,
 target.SecondaryClaimPayerKey = source.SecondaryClaimPayerKey,
 target.TertiaryClaimPayerKey = source.TertiaryClaimPayerKey,
 target.LiabilityClaimPayerKey = source.LiabilityClaimPayerKey,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDatetime = source.DWLastUpdateDatetime
WHEN NOT MATCHED THEN
  INSERT (ClaimKey, PrimaryClaimPayerKey, SecondaryClaimPayerKey, TertiaryClaimPayerKey, LiabilityClaimPayerKey, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDatetime)
  VALUES (source.ClaimKey, source.PrimaryClaimPayerKey, source.SecondaryClaimPayerKey, source.TertiaryClaimPayerKey, source.LiabilityClaimPayerKey, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDatetime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_JuncFactClaimToPayer
      GROUP BY ClaimKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_JuncFactClaimToPayer');
ELSE
  COMMIT TRANSACTION;
END IF;
