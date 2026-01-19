
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefCPTCode AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefCPTCode AS source
ON target.CPTCodeKey = source.CPTCodeKey
WHEN MATCHED THEN
  UPDATE SET
  target.CPTCodeKey = source.CPTCodeKey,
 target.CPTCode = TRIM(source.CPTCode),
 target.CPTName = TRIM(source.CPTName),
 target.DeleteFlag = source.DeleteFlag,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.Category1Code = TRIM(source.Category1Code),
 target.Category2Code = TRIM(source.Category2Code)
WHEN NOT MATCHED THEN
  INSERT (CPTCodeKey, CPTCode, CPTName, DeleteFlag, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, Category1Code, Category2Code)
  VALUES (source.CPTCodeKey, TRIM(source.CPTCode), TRIM(source.CPTName), source.DeleteFlag, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.Category1Code), TRIM(source.Category2Code));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CPTCodeKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefCPTCode
      GROUP BY CPTCodeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefCPTCode');
ELSE
  COMMIT TRANSACTION;
END IF;
