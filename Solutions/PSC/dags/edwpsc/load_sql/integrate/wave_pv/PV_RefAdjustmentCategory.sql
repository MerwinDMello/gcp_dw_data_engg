
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_RefAdjustmentCategory AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_RefAdjustmentCategory AS source
ON target.AdjustmentCategoryKey = source.AdjustmentCategoryKey
WHEN MATCHED THEN
  UPDATE SET
  target.AdjustmentCategoryKey = source.AdjustmentCategoryKey,
 target.AdjCategoryName = TRIM(source.AdjCategoryName),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (AdjustmentCategoryKey, AdjCategoryName, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.AdjustmentCategoryKey, TRIM(source.AdjCategoryName), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT AdjustmentCategoryKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_RefAdjustmentCategory
      GROUP BY AdjustmentCategoryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_RefAdjustmentCategory');
ELSE
  COMMIT TRANSACTION;
END IF;
