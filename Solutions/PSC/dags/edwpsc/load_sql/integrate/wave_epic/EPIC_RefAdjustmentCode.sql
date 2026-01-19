
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefAdjustmentCode AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefAdjustmentCode AS source
ON target.AdjustmentCodeKey = source.AdjustmentCodeKey
WHEN MATCHED THEN
  UPDATE SET
  target.AdjustmentCodeKey = source.AdjustmentCodeKey,
 target.AdjCode = TRIM(source.AdjCode),
 target.AdjName = TRIM(source.AdjName),
 target.AdjDescription = TRIM(source.AdjDescription),
 target.AdjShortName = TRIM(source.AdjShortName),
 target.AdjActive = source.AdjActive,
 target.ProcId = TRIM(source.ProcId),
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKey = TRIM(source.SourceAPrimaryKey),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.AdjustmentCategoryKey = source.AdjustmentCategoryKey,
 target.NonParFlag = source.NonParFlag,
 target.BillableFlag = source.BillableFlag,
 target.AdjSubCategoryName = TRIM(source.AdjSubCategoryName)
WHEN NOT MATCHED THEN
  INSERT (AdjustmentCodeKey, AdjCode, AdjName, AdjDescription, AdjShortName, AdjActive, ProcId, RegionKey, SourceAPrimaryKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, AdjustmentCategoryKey, NonParFlag, BillableFlag, AdjSubCategoryName)
  VALUES (source.AdjustmentCodeKey, TRIM(source.AdjCode), TRIM(source.AdjName), TRIM(source.AdjDescription), TRIM(source.AdjShortName), source.AdjActive, TRIM(source.ProcId), source.RegionKey, TRIM(source.SourceAPrimaryKey), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.AdjustmentCategoryKey, source.NonParFlag, source.BillableFlag, TRIM(source.AdjSubCategoryName));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT AdjustmentCodeKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefAdjustmentCode
      GROUP BY AdjustmentCodeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefAdjustmentCode');
ELSE
  COMMIT TRANSACTION;
END IF;
