
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_JuncCPTCodeCoid AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_JuncCPTCodeCoid AS source
ON target.JuncCPTCodeCoidKey = source.JuncCPTCodeCoidKey
WHEN MATCHED THEN
  UPDATE SET
  target.JuncCPTCodeCoidKey = source.JuncCPTCodeCoidKey,
 target.CPTCodeKey = source.CPTCodeKey,
 target.Coid = TRIM(source.Coid),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (JuncCPTCodeCoidKey, CPTCodeKey, Coid, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.JuncCPTCodeCoidKey, source.CPTCodeKey, TRIM(source.Coid), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT JuncCPTCodeCoidKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_JuncCPTCodeCoid
      GROUP BY JuncCPTCodeCoidKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_JuncCPTCodeCoid');
ELSE
  COMMIT TRANSACTION;
END IF;
