
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefCoidSum300 AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefCoidSum300 AS source
ON target.COID = source.COID
WHEN MATCHED THEN
  UPDATE SET
  target.Coid = TRIM(source.Coid),
 target.FEIN = TRIM(source.FEIN),
 target.HCAPS300 = TRIM(source.HCAPS300),
 target.ConsCoid = TRIM(source.ConsCoid),
 target.FLevel = TRIM(source.FLevel),
 target.LegalName = TRIM(source.LegalName),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (Coid, FEIN, HCAPS300, ConsCoid, FLevel, LegalName, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (TRIM(source.Coid), TRIM(source.FEIN), TRIM(source.HCAPS300), TRIM(source.ConsCoid), TRIM(source.FLevel), TRIM(source.LegalName), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT COID
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefCoidSum300
      GROUP BY COID
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefCoidSum300');
ELSE
  COMMIT TRANSACTION;
END IF;
