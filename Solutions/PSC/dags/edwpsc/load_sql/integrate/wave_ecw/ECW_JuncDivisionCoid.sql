
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncDivisionCoid AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_JuncDivisionCoid AS source
ON target.JuncDivisionCoidKey = source.JuncDivisionCoidKey
WHEN MATCHED THEN
  UPDATE SET
  target.JuncDivisionCoidKey = source.JuncDivisionCoidKey,
 target.DivisionKey = source.DivisionKey,
 target.Coid = TRIM(source.Coid),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (JuncDivisionCoidKey, DivisionKey, Coid, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.JuncDivisionCoidKey, source.DivisionKey, TRIM(source.Coid), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT JuncDivisionCoidKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_JuncDivisionCoid
      GROUP BY JuncDivisionCoidKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_JuncDivisionCoid');
ELSE
  COMMIT TRANSACTION;
END IF;
