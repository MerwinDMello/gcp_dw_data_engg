
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PK_FactChangeData AS target
USING {{ params.param_psc_stage_dataset_name }}.PK_FactChangeData AS source
ON target.PKChangeDataKey = source.PKChangeDataKey
WHEN MATCHED THEN
  UPDATE SET
  target.PKChangeDataKey = source.PKChangeDataKey,
 target.CreatedTime = source.CreatedTime,
 target.ModifiedTime = source.ModifiedTime,
 target.PKTransactionDetailsKey = source.PKTransactionDetailsKey,
 target.ChargeId = source.ChargeId,
 target.FieldId = source.FieldId,
 target.FieldCategory = TRIM(source.FieldCategory),
 target.FieldValueText = TRIM(source.FieldValueText),
 target.FieldValueId = source.FieldValueId,
 target.SyncVersion = source.SyncVersion,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceBPrimaryKeyValue = source.SourceBPrimaryKeyValue,
 target.PKRegionName = TRIM(source.PKRegionName),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (PKChangeDataKey, CreatedTime, ModifiedTime, PKTransactionDetailsKey, ChargeId, FieldId, FieldCategory, FieldValueText, FieldValueId, SyncVersion, SourceAPrimaryKeyValue, SourceBPrimaryKeyValue, PKRegionName, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.PKChangeDataKey, source.CreatedTime, source.ModifiedTime, source.PKTransactionDetailsKey, source.ChargeId, source.FieldId, TRIM(source.FieldCategory), TRIM(source.FieldValueText), source.FieldValueId, source.SyncVersion, source.SourceAPrimaryKeyValue, source.SourceBPrimaryKeyValue, TRIM(source.PKRegionName), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PKChangeDataKey
      FROM {{ params.param_psc_core_dataset_name }}.PK_FactChangeData
      GROUP BY PKChangeDataKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PK_FactChangeData');
ELSE
  COMMIT TRANSACTION;
END IF;
