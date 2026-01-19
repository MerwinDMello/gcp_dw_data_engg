
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefCPTWorkRVU AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefCPTWorkRVU AS source
ON target.CPTRVUKey = source.CPTRVUKey
WHEN MATCHED THEN
  UPDATE SET
  target.CPTRVUKey = source.CPTRVUKey,
 target.CPTCode = TRIM(source.CPTCode),
 target.YEAR = source.YEAR,
 target.CPTModifier = TRIM(source.CPTModifier),
 target.CPTDescription = TRIM(source.CPTDescription),
 target.WorkRVUStat = source.WorkRVUStat,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (CPTRVUKey, CPTCode, YEAR, CPTModifier, CPTDescription, WorkRVUStat, DWLastUpdateDateTime)
  VALUES (source.CPTRVUKey, TRIM(source.CPTCode), source.YEAR, TRIM(source.CPTModifier), TRIM(source.CPTDescription), source.WorkRVUStat, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CPTRVUKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefCPTWorkRVU
      GROUP BY CPTRVUKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefCPTWorkRVU');
ELSE
  COMMIT TRANSACTION;
END IF;
