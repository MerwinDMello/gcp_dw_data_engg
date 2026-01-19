
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefProvider AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefProvider AS source
ON target.ProviderKey = source.ProviderKey
WHEN MATCHED THEN
  UPDATE SET
  target.ProviderKey = source.ProviderKey,
 target.ProviderFullName = TRIM(source.ProviderFullName),
 target.ProviderFirstName = TRIM(source.ProviderFirstName),
 target.ProviderMiddleName = TRIM(source.ProviderMiddleName),
 target.ProviderLastName = TRIM(source.ProviderLastName),
 target.ProviderPrimaryAddressLine1 = TRIM(source.ProviderPrimaryAddressLine1),
 target.ProviderPrimaryAddressLine2 = TRIM(source.ProviderPrimaryAddressLine2),
 target.ProviderPrimaryAddressLine3 = TRIM(source.ProviderPrimaryAddressLine3),
 target.ProviderPrimaryCity = TRIM(source.ProviderPrimaryCity),
 target.ProviderPrimaryState = TRIM(source.ProviderPrimaryState),
 target.ProviderPrimaryZip = TRIM(source.ProviderPrimaryZip),
 target.ProviderPhone = TRIM(source.ProviderPhone),
 target.ProviderFax = TRIM(source.ProviderFax),
 target.ProviderEmail = TRIM(source.ProviderEmail),
 target.ProviderNPI = TRIM(source.ProviderNPI),
 target.ProviderUPIN = TRIM(source.ProviderUPIN),
 target.ProviderPrintName = TRIM(source.ProviderPrintName),
 target.ProviderUserId = TRIM(source.ProviderUserId),
 target.ProviderClinicianTitle = TRIM(source.ProviderClinicianTitle),
 target.ProviderActiveFlag = source.ProviderActiveFlag,
 target.ProviderType = TRIM(source.ProviderType),
 target.ProviderSpeciality = TRIM(source.ProviderSpeciality),
 target.ProviderSex = TRIM(source.ProviderSex),
 target.ProviderDOB = TRIM(source.ProviderDOB),
 target.DeleteFlag = source.DeleteFlag,
 target.RegionKey = source.RegionKey,
 target.ProvId = TRIM(source.ProvId),
 target.SerRefSrceId = TRIM(source.SerRefSrceId),
 target.EpicProviderId = TRIM(source.EpicProviderId),
 target.ProviderCID = TRIM(source.ProviderCID),
 target.ProviderInternalId = TRIM(source.ProviderInternalId),
 target.SourceAPrimaryKey = TRIM(source.SourceAPrimaryKey),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.ProviderPrimaryGeographyKey = source.ProviderPrimaryGeographyKey,
 target.ProviderUserType = source.ProviderUserType,
 target.ProviderInitials = TRIM(source.ProviderInitials),
 target.ProviderDOBDateKey = source.ProviderDOBDateKey,
 target.ProviderSSN = TRIM(source.ProviderSSN),
 target.ProviderMobilePhone = TRIM(source.ProviderMobilePhone),
 target.ProviderPager = TRIM(source.ProviderPager),
 target.ProviderPrimaryServiceLocation = source.ProviderPrimaryServiceLocation,
 target.ProviderSpecialityCode = TRIM(source.ProviderSpecialityCode),
 target.ProviderCode = TRIM(source.ProviderCode),
 target.ProviderTaxID = TRIM(source.ProviderTaxID),
 target.ProviderTaxIDType = TRIM(source.ProviderTaxIDType),
 target.ProviderTaxIDSuffix = TRIM(source.ProviderTaxIDSuffix),
 target.ProviderTaxonomyCode = TRIM(source.ProviderTaxonomyCode),
 target.ProviderFacilityKey = source.ProviderFacilityKey,
 target.ProviderDeaNo = TRIM(source.ProviderDeaNo),
 target.ProviderSpLicNo = TRIM(source.ProviderSpLicNo),
 target.ProviderStLicNo = TRIM(source.ProviderStLicNo),
 target.ProviderAneLicNo = TRIM(source.ProviderAneLicNo),
 target.ProviderDoctorType = TRIM(source.ProviderDoctorType),
 target.ProviderRoleType = TRIM(source.ProviderRoleType),
 target.ProviderGrpNPI = TRIM(source.ProviderGrpNPI),
 target.ProviderInactive = source.ProviderInactive,
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBPrimaryKeyValue = source.SourceBPrimaryKeyValue,
 target.SourceBRecordLastUpdated = source.SourceBRecordLastUpdated
WHEN NOT MATCHED THEN
  INSERT (ProviderKey, ProviderFullName, ProviderFirstName, ProviderMiddleName, ProviderLastName, ProviderPrimaryAddressLine1, ProviderPrimaryAddressLine2, ProviderPrimaryAddressLine3, ProviderPrimaryCity, ProviderPrimaryState, ProviderPrimaryZip, ProviderPhone, ProviderFax, ProviderEmail, ProviderNPI, ProviderUPIN, ProviderPrintName, ProviderUserId, ProviderClinicianTitle, ProviderActiveFlag, ProviderType, ProviderSpeciality, ProviderSex, ProviderDOB, DeleteFlag, RegionKey, ProvId, SerRefSrceId, EpicProviderId, ProviderCID, ProviderInternalId, SourceAPrimaryKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ProviderPrimaryGeographyKey, ProviderUserType, ProviderInitials, ProviderDOBDateKey, ProviderSSN, ProviderMobilePhone, ProviderPager, ProviderPrimaryServiceLocation, ProviderSpecialityCode, ProviderCode, ProviderTaxID, ProviderTaxIDType, ProviderTaxIDSuffix, ProviderTaxonomyCode, ProviderFacilityKey, ProviderDeaNo, ProviderSpLicNo, ProviderStLicNo, ProviderAneLicNo, ProviderDoctorType, ProviderRoleType, ProviderGrpNPI, ProviderInactive, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBPrimaryKeyValue, SourceBRecordLastUpdated)
  VALUES (source.ProviderKey, TRIM(source.ProviderFullName), TRIM(source.ProviderFirstName), TRIM(source.ProviderMiddleName), TRIM(source.ProviderLastName), TRIM(source.ProviderPrimaryAddressLine1), TRIM(source.ProviderPrimaryAddressLine2), TRIM(source.ProviderPrimaryAddressLine3), TRIM(source.ProviderPrimaryCity), TRIM(source.ProviderPrimaryState), TRIM(source.ProviderPrimaryZip), TRIM(source.ProviderPhone), TRIM(source.ProviderFax), TRIM(source.ProviderEmail), TRIM(source.ProviderNPI), TRIM(source.ProviderUPIN), TRIM(source.ProviderPrintName), TRIM(source.ProviderUserId), TRIM(source.ProviderClinicianTitle), source.ProviderActiveFlag, TRIM(source.ProviderType), TRIM(source.ProviderSpeciality), TRIM(source.ProviderSex), TRIM(source.ProviderDOB), source.DeleteFlag, source.RegionKey, TRIM(source.ProvId), TRIM(source.SerRefSrceId), TRIM(source.EpicProviderId), TRIM(source.ProviderCID), TRIM(source.ProviderInternalId), TRIM(source.SourceAPrimaryKey), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.ProviderPrimaryGeographyKey, source.ProviderUserType, TRIM(source.ProviderInitials), source.ProviderDOBDateKey, TRIM(source.ProviderSSN), TRIM(source.ProviderMobilePhone), TRIM(source.ProviderPager), source.ProviderPrimaryServiceLocation, TRIM(source.ProviderSpecialityCode), TRIM(source.ProviderCode), TRIM(source.ProviderTaxID), TRIM(source.ProviderTaxIDType), TRIM(source.ProviderTaxIDSuffix), TRIM(source.ProviderTaxonomyCode), source.ProviderFacilityKey, TRIM(source.ProviderDeaNo), TRIM(source.ProviderSpLicNo), TRIM(source.ProviderStLicNo), TRIM(source.ProviderAneLicNo), TRIM(source.ProviderDoctorType), TRIM(source.ProviderRoleType), TRIM(source.ProviderGrpNPI), source.ProviderInactive, TRIM(source.SourceAPrimaryKeyValue), source.SourceARecordLastUpdated, source.SourceBPrimaryKeyValue, source.SourceBRecordLastUpdated);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ProviderKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefProvider
      GROUP BY ProviderKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefProvider');
ELSE
  COMMIT TRANSACTION;
END IF;
