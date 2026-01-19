
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefUsersMeditechExpanse AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefUsersMeditechExpanse AS source
ON target.UserKey = source.UserKey
WHEN MATCHED THEN
  UPDATE SET
  target.UserKey = source.UserKey,
 target.RegionKey = source.RegionKey,
 target.UserName = TRIM(source.UserName),
 target.UserFirstName = TRIM(source.UserFirstName),
 target.UserLastName = TRIM(source.UserLastName),
 target.UserMiddleName = TRIM(source.UserMiddleName),
 target.UserFullName = TRIM(source.UserFullName),
 target.UserProfileID = TRIM(source.UserProfileID),
 target.UserProviderID = TRIM(source.UserProviderID),
 target.DeleteFlag = source.DeleteFlag,
 target.SourcePrimaryKeyValue = TRIM(source.SourcePrimaryKeyValue),
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (UserKey, RegionKey, UserName, UserFirstName, UserLastName, UserMiddleName, UserFullName, UserProfileID, UserProviderID, DeleteFlag, SourcePrimaryKeyValue, SourceARecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.UserKey, source.RegionKey, TRIM(source.UserName), TRIM(source.UserFirstName), TRIM(source.UserLastName), TRIM(source.UserMiddleName), TRIM(source.UserFullName), TRIM(source.UserProfileID), TRIM(source.UserProviderID), source.DeleteFlag, TRIM(source.SourcePrimaryKeyValue), source.SourceARecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT UserKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefUsersMeditechExpanse
      GROUP BY UserKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefUsersMeditechExpanse');
ELSE
  COMMIT TRANSACTION;
END IF;
