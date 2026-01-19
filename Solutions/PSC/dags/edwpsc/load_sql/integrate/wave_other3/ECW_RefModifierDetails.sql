
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefModifierDetails AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefModifierDetails AS source
ON target.ModifierDetailKey = source.ModifierDetailKey
WHEN MATCHED THEN
  UPDATE SET
  target.ModifierDetailKey = source.ModifierDetailKey,
 target.ModifierCode = TRIM(source.ModifierCode),
 target.ModifierDescription = TRIM(source.ModifierDescription),
 target.AnesthesiaPercentage = source.AnesthesiaPercentage,
 target.AnesthesiaSuppressFlag = source.AnesthesiaSuppressFlag,
 target.AnesthesiaPhysicalStatusUnits = source.AnesthesiaPhysicalStatusUnits,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.SourcePrimaryKeyValue = TRIM(source.SourcePrimaryKeyValue),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ModifierDetailKey, ModifierCode, ModifierDescription, AnesthesiaPercentage, AnesthesiaSuppressFlag, AnesthesiaPhysicalStatusUnits, SourceSystemCode, SourcePrimaryKeyValue, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ModifierDetailKey, TRIM(source.ModifierCode), TRIM(source.ModifierDescription), source.AnesthesiaPercentage, source.AnesthesiaSuppressFlag, source.AnesthesiaPhysicalStatusUnits, TRIM(source.SourceSystemCode), TRIM(source.SourcePrimaryKeyValue), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ModifierDetailKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefModifierDetails
      GROUP BY ModifierDetailKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefModifierDetails');
ELSE
  COMMIT TRANSACTION;
END IF;
