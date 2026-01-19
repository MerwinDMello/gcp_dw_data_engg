
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefVisitStatus_History AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefVisitStatus_History AS source
ON target.VisitStatusKey = source.VisitStatusKey AND target.SysStartTime = source.SysStartTime AND target.SysEndTime = source.SysEndTime
WHEN MATCHED THEN
  UPDATE SET
  target.VisitStatusKey = source.VisitStatusKey,
 target.VisitStatusName = TRIM(source.VisitStatusName),
 target.VisitStatusDescription = TRIM(source.VisitStatusDescription),
 target.VisitStatusNonBillable = source.VisitStatusNonBillable,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
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
  INSERT (VisitStatusKey, VisitStatusName, VisitStatusDescription, VisitStatusNonBillable, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag, SysStartTime, SysEndTime)
  VALUES (source.VisitStatusKey, TRIM(source.VisitStatusName), TRIM(source.VisitStatusDescription), source.VisitStatusNonBillable, source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag, source.SysStartTime, source.SysEndTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT VisitStatusKey, SysStartTime, SysEndTime
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefVisitStatus_History
      GROUP BY VisitStatusKey, SysStartTime, SysEndTime
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefVisitStatus_History');
ELSE
  COMMIT TRANSACTION;
END IF;
