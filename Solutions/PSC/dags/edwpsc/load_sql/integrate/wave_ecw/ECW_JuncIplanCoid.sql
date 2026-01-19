
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncIplanCoid AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_JuncIplanCoid AS source
ON target.JuncIplanCoidKey = source.JuncIplanCoidKey
WHEN MATCHED THEN
  UPDATE SET
  target.JuncIplanCoidKey = source.JuncIplanCoidKey,
 target.IplanKey = source.IplanKey,
 target.Coid = TRIM(source.Coid),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (JuncIplanCoidKey, IplanKey, Coid, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.JuncIplanCoidKey, source.IplanKey, TRIM(source.Coid), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT JuncIplanCoidKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_JuncIplanCoid
      GROUP BY JuncIplanCoidKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_JuncIplanCoid');
ELSE
  COMMIT TRANSACTION;
END IF;
