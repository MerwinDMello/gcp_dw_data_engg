
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefUser AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefUser AS source
ON target.UserKey = source.UserKey
WHEN MATCHED THEN
  UPDATE SET
  target.UserKey = source.UserKey,
 target.UserName = TRIM(source.UserName),
 target.UserFirstName = TRIM(source.UserFirstName),
 target.UserMiddleName = TRIM(source.UserMiddleName),
 target.UserLastName = TRIM(source.UserLastName),
 target.User34 = TRIM(source.User34),
 target.ActiveFlag = source.ActiveFlag,
 target.DeleteFlag = source.DeleteFlag,
 target.UserAddress = TRIM(source.UserAddress),
 target.UserCity = TRIM(source.UserCity),
 target.UserState = TRIM(source.UserState),
 target.UserZip = TRIM(source.UserZip),
 target.UserPhone = TRIM(source.UserPhone),
 target.UserAlias = TRIM(source.UserAlias),
 target.EpicEmpId = TRIM(source.EpicEmpId),
 target.UserId = TRIM(source.UserId),
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKey = TRIM(source.SourceAPrimaryKey),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.EpicUserName = TRIM(source.EpicUserName),
 target.UserPrimaryServiceLocation = source.UserPrimaryServiceLocation,
 target.UserType = source.UserType
WHEN NOT MATCHED THEN
  INSERT (UserKey, UserName, UserFirstName, UserMiddleName, UserLastName, User34, ActiveFlag, DeleteFlag, UserAddress, UserCity, UserState, UserZip, UserPhone, UserAlias, EpicEmpId, UserId, RegionKey, SourceAPrimaryKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, EpicUserName, UserPrimaryServiceLocation, UserType)
  VALUES (source.UserKey, TRIM(source.UserName), TRIM(source.UserFirstName), TRIM(source.UserMiddleName), TRIM(source.UserLastName), TRIM(source.User34), source.ActiveFlag, source.DeleteFlag, TRIM(source.UserAddress), TRIM(source.UserCity), TRIM(source.UserState), TRIM(source.UserZip), TRIM(source.UserPhone), TRIM(source.UserAlias), TRIM(source.EpicEmpId), TRIM(source.UserId), source.RegionKey, TRIM(source.SourceAPrimaryKey), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.EpicUserName), source.UserPrimaryServiceLocation, source.UserType);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT UserKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefUser
      GROUP BY UserKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefUser');
ELSE
  COMMIT TRANSACTION;
END IF;
