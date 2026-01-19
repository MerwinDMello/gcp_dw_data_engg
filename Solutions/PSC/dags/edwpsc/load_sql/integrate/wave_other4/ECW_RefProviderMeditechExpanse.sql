
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefProviderMeditechExpanse AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefProviderMeditechExpanse AS source
ON target.ProviderKey = source.ProviderKey
WHEN MATCHED THEN
  UPDATE SET
  target.ProviderKey = source.ProviderKey,
 target.RegionKey = source.RegionKey,
 target.ProviderFirstName = TRIM(source.ProviderFirstName),
 target.ProviderLastName = TRIM(source.ProviderLastName),
 target.ProviderMiddleName = TRIM(source.ProviderMiddleName),
 target.ProviderUserName = TRIM(source.ProviderUserName),
 target.ProviderName = TRIM(source.ProviderName),
 target.ProviderAddressLine1 = TRIM(source.ProviderAddressLine1),
 target.ProviderAddressLine2 = TRIM(source.ProviderAddressLine2),
 target.ProviderGeographyKey = source.ProviderGeographyKey,
 target.ProviderSpeciality = TRIM(source.ProviderSpeciality),
 target.ProviderLicense = TRIM(source.ProviderLicense),
 target.ProviderGroupID = TRIM(source.ProviderGroupID),
 target.ProviderNPI = TRIM(source.ProviderNPI),
 target.ProviderServiceID = TRIM(source.ProviderServiceID),
 target.ProviderType = TRIM(source.ProviderType),
 target.DeleteFlag = source.DeleteFlag,
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ProviderKey, RegionKey, ProviderFirstName, ProviderLastName, ProviderMiddleName, ProviderUserName, ProviderName, ProviderAddressLine1, ProviderAddressLine2, ProviderGeographyKey, ProviderSpeciality, ProviderLicense, ProviderGroupID, ProviderNPI, ProviderServiceID, ProviderType, DeleteFlag, SourceAPrimaryKeyValue, SourceARecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ProviderKey, source.RegionKey, TRIM(source.ProviderFirstName), TRIM(source.ProviderLastName), TRIM(source.ProviderMiddleName), TRIM(source.ProviderUserName), TRIM(source.ProviderName), TRIM(source.ProviderAddressLine1), TRIM(source.ProviderAddressLine2), source.ProviderGeographyKey, TRIM(source.ProviderSpeciality), TRIM(source.ProviderLicense), TRIM(source.ProviderGroupID), TRIM(source.ProviderNPI), TRIM(source.ProviderServiceID), TRIM(source.ProviderType), source.DeleteFlag, TRIM(source.SourceAPrimaryKeyValue), source.SourceARecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ProviderKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefProviderMeditechExpanse
      GROUP BY ProviderKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefProviderMeditechExpanse');
ELSE
  COMMIT TRANSACTION;
END IF;
