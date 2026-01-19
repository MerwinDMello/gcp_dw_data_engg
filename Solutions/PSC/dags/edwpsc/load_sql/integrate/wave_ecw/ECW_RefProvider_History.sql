
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefProvider_History AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefProvider_History AS source
ON target.ProviderKey = source.ProviderKey AND target.SysStartTime = source.SysStartTime AND target.SysEndTime = source.SysEndTime
WHEN MATCHED THEN
  UPDATE SET
  target.ProviderKey = source.ProviderKey,
 target.ProviderFirstName = TRIM(source.ProviderFirstName),
 target.ProviderLastName = TRIM(source.ProviderLastName),
 target.ProviderMiddleName = TRIM(source.ProviderMiddleName),
 target.ProviderUserName = TRIM(source.ProviderUserName),
 target.ProviderPrefixName = TRIM(source.ProviderPrefixName),
 target.ProviderSuffixName = TRIM(source.ProviderSuffixName),
 target.ProviderPrimaryAddressLine1 = TRIM(source.ProviderPrimaryAddressLine1),
 target.ProviderPrimaryAddressLine2 = TRIM(source.ProviderPrimaryAddressLine2),
 target.ProviderPrimaryGeographyKey = source.ProviderPrimaryGeographyKey,
 target.ProviderUserType = source.ProviderUserType,
 target.ProviderInitials = TRIM(source.ProviderInitials),
 target.ProviderDOBDateKey = source.ProviderDOBDateKey,
 target.ProviderSSN = TRIM(source.ProviderSSN),
 target.ProviderPrimaryServiceLocation = source.ProviderPrimaryServiceLocation,
 target.ProviderPhone = TRIM(source.ProviderPhone),
 target.ProviderMobilePhone = TRIM(source.ProviderMobilePhone),
 target.ProviderPager = TRIM(source.ProviderPager),
 target.ProviderEmail = TRIM(source.ProviderEmail),
 target.ProviderDeaNo = TRIM(source.ProviderDeaNo),
 target.ProviderFaxNo = TRIM(source.ProviderFaxNo),
 target.ProviderSpeciality = TRIM(source.ProviderSpeciality),
 target.ProviderSpecialityCode = TRIM(source.ProviderSpecialityCode),
 target.ProviderPrintName = TRIM(source.ProviderPrintName),
 target.ProviderCode = TRIM(source.ProviderCode),
 target.ProviderTaxID = TRIM(source.ProviderTaxID),
 target.ProviderTaxIDType = TRIM(source.ProviderTaxIDType),
 target.ProviderTaxIDSuffix = TRIM(source.ProviderTaxIDSuffix),
 target.ProviderNPI = TRIM(source.ProviderNPI),
 target.ProviderUPIN = TRIM(source.ProviderUPIN),
 target.ProviderTaxonomyCode = TRIM(source.ProviderTaxonomyCode),
 target.ProviderFacilityKey = source.ProviderFacilityKey,
 target.ProviderSpLicNo = TRIM(source.ProviderSpLicNo),
 target.ProviderStLicNo = TRIM(source.ProviderStLicNo),
 target.ProviderAneLicNo = TRIM(source.ProviderAneLicNo),
 target.ProviderDoctorType = TRIM(source.ProviderDoctorType),
 target.ProviderRoleType = TRIM(source.ProviderRoleType),
 target.ProviderGrpNPI = TRIM(source.ProviderGrpNPI),
 target.ProviderType = TRIM(source.ProviderType),
 target.ProviderInactive = source.ProviderInactive,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBPrimaryKeyValue = source.SourceBPrimaryKeyValue,
 target.SourceBRecordLastUpdated = source.SourceBRecordLastUpdated,
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
  INSERT (ProviderKey, ProviderFirstName, ProviderLastName, ProviderMiddleName, ProviderUserName, ProviderPrefixName, ProviderSuffixName, ProviderPrimaryAddressLine1, ProviderPrimaryAddressLine2, ProviderPrimaryGeographyKey, ProviderUserType, ProviderInitials, ProviderDOBDateKey, ProviderSSN, ProviderPrimaryServiceLocation, ProviderPhone, ProviderMobilePhone, ProviderPager, ProviderEmail, ProviderDeaNo, ProviderFaxNo, ProviderSpeciality, ProviderSpecialityCode, ProviderPrintName, ProviderCode, ProviderTaxID, ProviderTaxIDType, ProviderTaxIDSuffix, ProviderNPI, ProviderUPIN, ProviderTaxonomyCode, ProviderFacilityKey, ProviderSpLicNo, ProviderStLicNo, ProviderAneLicNo, ProviderDoctorType, ProviderRoleType, ProviderGrpNPI, ProviderType, ProviderInactive, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBPrimaryKeyValue, SourceBRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag, RegionKey, SysStartTime, SysEndTime)
  VALUES (source.ProviderKey, TRIM(source.ProviderFirstName), TRIM(source.ProviderLastName), TRIM(source.ProviderMiddleName), TRIM(source.ProviderUserName), TRIM(source.ProviderPrefixName), TRIM(source.ProviderSuffixName), TRIM(source.ProviderPrimaryAddressLine1), TRIM(source.ProviderPrimaryAddressLine2), source.ProviderPrimaryGeographyKey, source.ProviderUserType, TRIM(source.ProviderInitials), source.ProviderDOBDateKey, TRIM(source.ProviderSSN), source.ProviderPrimaryServiceLocation, TRIM(source.ProviderPhone), TRIM(source.ProviderMobilePhone), TRIM(source.ProviderPager), TRIM(source.ProviderEmail), TRIM(source.ProviderDeaNo), TRIM(source.ProviderFaxNo), TRIM(source.ProviderSpeciality), TRIM(source.ProviderSpecialityCode), TRIM(source.ProviderPrintName), TRIM(source.ProviderCode), TRIM(source.ProviderTaxID), TRIM(source.ProviderTaxIDType), TRIM(source.ProviderTaxIDSuffix), TRIM(source.ProviderNPI), TRIM(source.ProviderUPIN), TRIM(source.ProviderTaxonomyCode), source.ProviderFacilityKey, TRIM(source.ProviderSpLicNo), TRIM(source.ProviderStLicNo), TRIM(source.ProviderAneLicNo), TRIM(source.ProviderDoctorType), TRIM(source.ProviderRoleType), TRIM(source.ProviderGrpNPI), TRIM(source.ProviderType), source.ProviderInactive, source.SourceAPrimaryKeyValue, source.SourceARecordLastUpdated, source.SourceBPrimaryKeyValue, source.SourceBRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag, source.RegionKey, source.SysStartTime, source.SysEndTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ProviderKey, SysStartTime, SysEndTime
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefProvider_History
      GROUP BY ProviderKey, SysStartTime, SysEndTime
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefProvider_History');
ELSE
  COMMIT TRANSACTION;
END IF;
