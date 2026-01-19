
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_RefCoidConfiguration_History AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_RefCoidConfiguration_History AS source
ON target.CoidConfigurationKey = source.CoidConfigurationKey AND target.SysStartTime = source.SysStartTime AND target.SysEndTime = source.SysEndTime
WHEN MATCHED THEN
  UPDATE SET
  target.CoidConfigurationKey = source.CoidConfigurationKey,
 target.Coid = TRIM(source.Coid),
 target.DeptCode = TRIM(source.DeptCode),
 target.CoidConfigurationProviderKey = source.CoidConfigurationProviderKey,
 target.CoidConfigurationFacilityKey = source.CoidConfigurationFacilityKey,
 target.CoidConfigurationPracticeKey = source.CoidConfigurationPracticeKey,
 target.SourcePrimaryKeyValue = TRIM(source.SourcePrimaryKeyValue),
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DeleteFlag = source.DeleteFlag,
 target.SysStartTime = source.SysStartTime,
 target.SysEndTime = source.SysEndTime
WHEN NOT MATCHED THEN
  INSERT (CoidConfigurationKey, Coid, DeptCode, CoidConfigurationProviderKey, CoidConfigurationFacilityKey, CoidConfigurationPracticeKey, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag, SysStartTime, SysEndTime)
  VALUES (source.CoidConfigurationKey, TRIM(source.Coid), TRIM(source.DeptCode), source.CoidConfigurationProviderKey, source.CoidConfigurationFacilityKey, source.CoidConfigurationPracticeKey, TRIM(source.SourcePrimaryKeyValue), source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag, source.SysStartTime, source.SysEndTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CoidConfigurationKey, SysStartTime, SysEndTime
      FROM {{ params.param_psc_core_dataset_name }}.PV_RefCoidConfiguration_History
      GROUP BY CoidConfigurationKey, SysStartTime, SysEndTime
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_RefCoidConfiguration_History');
ELSE
  COMMIT TRANSACTION;
END IF;
