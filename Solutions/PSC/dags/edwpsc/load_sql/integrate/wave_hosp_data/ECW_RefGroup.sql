
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefGroup AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefGroup AS source
ON target.GroupKey = source.GroupKey
WHEN MATCHED THEN
  UPDATE SET
  target.GroupKey = source.GroupKey,
 target.GroupName = TRIM(source.GroupName),
 target.GroupIsVisibleOnFactClaim = source.GroupIsVisibleOnFactClaim,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.GroupCode = TRIM(source.GroupCode),
 target.DeleteFlag = source.DeleteFlag,
 target.coidstatflag = source.coidstatflag,
 target.PPMSFlag = source.PPMSFlag,
 target.SysStartTime = source.SysStartTime,
 target.SysEndTime = source.SysEndTime
WHEN NOT MATCHED THEN
  INSERT (GroupKey, GroupName, GroupIsVisibleOnFactClaim, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, GroupCode, DeleteFlag, coidstatflag, PPMSFlag, SysStartTime, SysEndTime)
  VALUES (source.GroupKey, TRIM(source.GroupName), source.GroupIsVisibleOnFactClaim, source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.GroupCode), source.DeleteFlag, source.coidstatflag, source.PPMSFlag, source.SysStartTime, source.SysEndTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT GroupKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefGroup
      GROUP BY GroupKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefGroup');
ELSE
  COMMIT TRANSACTION;
END IF;
