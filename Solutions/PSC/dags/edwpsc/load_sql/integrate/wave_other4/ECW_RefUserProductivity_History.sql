
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefUserProductivity_History AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefUserProductivity_History AS source
ON target.UserProductivityKey = source.UserProductivityKey AND target.SysStartTime = source.SysStartTime AND target.SysEndTime = source.SysEndTime
WHEN MATCHED THEN
  UPDATE SET
  target.UserProductivityKey = source.UserProductivityKey,
 target.UserProductivityName = TRIM(source.UserProductivityName),
 target.UserProductivityType = TRIM(source.UserProductivityType),
 target.UserProductivityDesc = TRIM(source.UserProductivityDesc),
 target.UserProductivityQuery = TRIM(source.UserProductivityQuery),
 target.UserProductivityCreatedBy = TRIM(source.UserProductivityCreatedBy),
 target.UserProductivityCreatedDateKey = source.UserProductivityCreatedDateKey,
 target.UserProductivityLastModifiedBy = TRIM(source.UserProductivityLastModifiedBy),
 target.UserProductivityLastProcessedDate = source.UserProductivityLastProcessedDate,
 target.UserProductivityLastErrorMessage = TRIM(source.UserProductivityLastErrorMessage),
 target.Enabled = source.Enabled,
 target.DeveloperEmail = TRIM(source.DeveloperEmail),
 target.SysStartTime = source.SysStartTime,
 target.SysEndTime = source.SysEndTime,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (UserProductivityKey, UserProductivityName, UserProductivityType, UserProductivityDesc, UserProductivityQuery, UserProductivityCreatedBy, UserProductivityCreatedDateKey, UserProductivityLastModifiedBy, UserProductivityLastProcessedDate, UserProductivityLastErrorMessage, Enabled, DeveloperEmail, SysStartTime, SysEndTime, DWLastUpdateDateTime)
  VALUES (source.UserProductivityKey, TRIM(source.UserProductivityName), TRIM(source.UserProductivityType), TRIM(source.UserProductivityDesc), TRIM(source.UserProductivityQuery), TRIM(source.UserProductivityCreatedBy), source.UserProductivityCreatedDateKey, TRIM(source.UserProductivityLastModifiedBy), source.UserProductivityLastProcessedDate, TRIM(source.UserProductivityLastErrorMessage), source.Enabled, TRIM(source.DeveloperEmail), source.SysStartTime, source.SysEndTime, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT UserProductivityKey, SysStartTime, SysEndTime
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefUserProductivity_History
      GROUP BY UserProductivityKey, SysStartTime, SysEndTime
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefUserProductivity_History');
ELSE
  COMMIT TRANSACTION;
END IF;
