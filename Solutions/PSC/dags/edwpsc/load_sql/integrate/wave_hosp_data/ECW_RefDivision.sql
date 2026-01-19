
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefDivision AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefDivision AS source
ON target.DivisionKey = source.DivisionKey
WHEN MATCHED THEN
  UPDATE SET
  target.DivisionKey = source.DivisionKey,
 target.DivisionName = TRIM(source.DivisionName),
 target.GroupKey = source.GroupKey,
 target.DivisionIsVisibleOnFactClaim = source.DivisionIsVisibleOnFactClaim,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DivisionCode = TRIM(source.DivisionCode),
 target.DeleteFlag = source.DeleteFlag,
 target.coidstatflag = source.coidstatflag,
 target.PPMSFlag = source.PPMSFlag,
 target.SysStartTime = source.SysStartTime,
 target.SysEndTime = source.SysEndTime
WHEN NOT MATCHED THEN
  INSERT (DivisionKey, DivisionName, GroupKey, DivisionIsVisibleOnFactClaim, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DivisionCode, DeleteFlag, coidstatflag, PPMSFlag, SysStartTime, SysEndTime)
  VALUES (source.DivisionKey, TRIM(source.DivisionName), source.GroupKey, source.DivisionIsVisibleOnFactClaim, source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.DivisionCode), source.DeleteFlag, source.coidstatflag, source.PPMSFlag, source.SysStartTime, source.SysEndTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT DivisionKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefDivision
      GROUP BY DivisionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefDivision');
ELSE
  COMMIT TRANSACTION;
END IF;
