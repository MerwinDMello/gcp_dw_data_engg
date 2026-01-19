
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_RefUser AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_RefUser AS source
ON target.UserKey = source.UserKey
WHEN MATCHED THEN
  UPDATE SET
  target.UserKey = source.UserKey,
 target.UserFirstName = TRIM(source.UserFirstName),
 target.UserLastName = TRIM(source.UserLastName),
 target.UserMiddleName = TRIM(source.UserMiddleName),
 target.UserName = TRIM(source.UserName),
 target.UserType = source.UserType,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.UserProfilePK = TRIM(source.UserProfilePK),
 target.UserStatus = TRIM(source.UserStatus),
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DeleteFlag = source.DeleteFlag,
 target.RegionKey = source.RegionKey,
 target.SysStartTime = source.SysStartTime,
 target.SysEndTime = source.SysEndTime
WHEN NOT MATCHED THEN
  INSERT (UserKey, UserFirstName, UserLastName, UserMiddleName, UserName, UserType, SourcePrimaryKeyValue, UserProfilePK, UserStatus, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag, RegionKey, SysStartTime, SysEndTime)
  VALUES (source.UserKey, TRIM(source.UserFirstName), TRIM(source.UserLastName), TRIM(source.UserMiddleName), TRIM(source.UserName), source.UserType, source.SourcePrimaryKeyValue, TRIM(source.UserProfilePK), TRIM(source.UserStatus), source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag, source.RegionKey, source.SysStartTime, source.SysEndTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT UserKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_RefUser
      GROUP BY UserKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_RefUser');
ELSE
  COMMIT TRANSACTION;
END IF;
