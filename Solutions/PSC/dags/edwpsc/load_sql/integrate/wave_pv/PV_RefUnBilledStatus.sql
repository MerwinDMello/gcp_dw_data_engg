
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_RefUnBilledStatus AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_RefUnBilledStatus AS source
ON target.UnBilledStatusKey = source.UnBilledStatusKey
WHEN MATCHED THEN
  UPDATE SET
  target.UnBilledStatusKey = source.UnBilledStatusKey,
 target.UnBilledStatusCategory = TRIM(source.UnBilledStatusCategory),
 target.UnBilledStatusSubCategory = TRIM(source.UnBilledStatusSubCategory),
 target.UnBilledOnHoldFlag = source.UnBilledOnHoldFlag,
 target.UnBilledUnbilledFlag = source.UnBilledUnbilledFlag,
 target.UnBilledBilledFlag = source.UnBilledBilledFlag,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DeleteFlag = source.DeleteFlag
WHEN NOT MATCHED THEN
  INSERT (UnBilledStatusKey, UnBilledStatusCategory, UnBilledStatusSubCategory, UnBilledOnHoldFlag, UnBilledUnbilledFlag, UnBilledBilledFlag, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag)
  VALUES (source.UnBilledStatusKey, TRIM(source.UnBilledStatusCategory), TRIM(source.UnBilledStatusSubCategory), source.UnBilledOnHoldFlag, source.UnBilledUnbilledFlag, source.UnBilledBilledFlag, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT UnBilledStatusKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_RefUnBilledStatus
      GROUP BY UnBilledStatusKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_RefUnBilledStatus');
ELSE
  COMMIT TRANSACTION;
END IF;
