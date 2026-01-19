
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefGLStatCode AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefGLStatCode AS source
ON target.GLStatCodeKey = source.GLStatCodeKey
WHEN MATCHED THEN
  UPDATE SET
  target.GLStatCodeKey = TRIM(source.GLStatCodeKey),
 target.StatCodeDesc = TRIM(source.StatCodeDesc),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (GLStatCodeKey, StatCodeDesc, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (TRIM(source.GLStatCodeKey), TRIM(source.StatCodeDesc), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT GLStatCodeKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefGLStatCode
      GROUP BY GLStatCodeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefGLStatCode');
ELSE
  COMMIT TRANSACTION;
END IF;
