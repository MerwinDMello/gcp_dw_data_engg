
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefSRTGroupValueControlNumber AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefSRTGroupValueControlNumber AS source
ON target.GroupValueControlNumberKey = source.GroupValueControlNumberKey
WHEN MATCHED THEN
  UPDATE SET
  target.GroupValueControlNumberKey = source.GroupValueControlNumberKey,
 target.PrimaryGroupValue = TRIM(source.PrimaryGroupValue),
 target.SecondaryGroupValue = TRIM(source.SecondaryGroupValue),
 target.Control1 = source.Control1,
 target.Control2 = source.Control2,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.DeleteFlag = source.DeleteFlag,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.PrimaryGroupSourceSystem = TRIM(source.PrimaryGroupSourceSystem),
 target.SecondaryGroupSourceSystem = TRIM(source.SecondaryGroupSourceSystem)
WHEN NOT MATCHED THEN
  INSERT (GroupValueControlNumberKey, PrimaryGroupValue, SecondaryGroupValue, Control1, Control2, SourceAPrimaryKeyValue, DeleteFlag, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, PrimaryGroupSourceSystem, SecondaryGroupSourceSystem)
  VALUES (source.GroupValueControlNumberKey, TRIM(source.PrimaryGroupValue), TRIM(source.SecondaryGroupValue), source.Control1, source.Control2, source.SourceAPrimaryKeyValue, source.DeleteFlag, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.PrimaryGroupSourceSystem), TRIM(source.SecondaryGroupSourceSystem));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT GroupValueControlNumberKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefSRTGroupValueControlNumber
      GROUP BY GroupValueControlNumberKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefSRTGroupValueControlNumber');
ELSE
  COMMIT TRANSACTION;
END IF;
