
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PK_RefProvider AS target
USING {{ params.param_psc_stage_dataset_name }}.PK_RefProvider AS source
ON target.PKProviderKey = source.PKProviderKey
WHEN MATCHED THEN
  UPDATE SET
  target.PKProviderKey = source.PKProviderKey,
 target.ProviderUserName = TRIM(source.ProviderUserName),
 target.ProviderDOMUserName = TRIM(source.ProviderDOMUserName),
 target.ProviderFirstName = TRIM(source.ProviderFirstName),
 target.ProviderLastName = TRIM(source.ProviderLastName),
 target.ProviderMiddleName = TRIM(source.ProviderMiddleName),
 target.ProviderFullName = TRIM(source.ProviderFullName),
 target.ProviderDEANumber = TRIM(source.ProviderDEANumber),
 target.ProviderNPI = TRIM(source.ProviderNPI),
 target.AuthIndicatorFlag = source.AuthIndicatorFlag,
 target.DeleteFlag = source.DeleteFlag,
 target.CreatedDateTime = source.CreatedDateTime,
 target.UpdatedDateTime = source.UpdatedDateTime,
 target.PKRegionName = TRIM(source.PKRegionName),
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (PKProviderKey, ProviderUserName, ProviderDOMUserName, ProviderFirstName, ProviderLastName, ProviderMiddleName, ProviderFullName, ProviderDEANumber, ProviderNPI, AuthIndicatorFlag, DeleteFlag, CreatedDateTime, UpdatedDateTime, PKRegionName, SourceAPrimaryKeyValue, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.PKProviderKey, TRIM(source.ProviderUserName), TRIM(source.ProviderDOMUserName), TRIM(source.ProviderFirstName), TRIM(source.ProviderLastName), TRIM(source.ProviderMiddleName), TRIM(source.ProviderFullName), TRIM(source.ProviderDEANumber), TRIM(source.ProviderNPI), source.AuthIndicatorFlag, source.DeleteFlag, source.CreatedDateTime, source.UpdatedDateTime, TRIM(source.PKRegionName), source.SourceAPrimaryKeyValue, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PKProviderKey
      FROM {{ params.param_psc_core_dataset_name }}.PK_RefProvider
      GROUP BY PKProviderKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PK_RefProvider');
ELSE
  COMMIT TRANSACTION;
END IF;
