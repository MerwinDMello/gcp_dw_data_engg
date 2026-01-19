
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefPatientMeditechExpanse AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefPatientMeditechExpanse AS source
ON target.PatientKey = source.PatientKey
WHEN MATCHED THEN
  UPDATE SET
  target.PatientKey = source.PatientKey,
 target.RegionKey = source.RegionKey,
 target.PatientDOBDateKey = source.PatientDOBDateKey,
 target.PatientGuarantorPatientKey = source.PatientGuarantorPatientKey,
 target.PatientGuarantorID = TRIM(source.PatientGuarantorID),
 target.PatientAttendingDoctorProviderKey = source.PatientAttendingDoctorProviderKey,
 target.PatientReferringProviderKey = source.PatientReferringProviderKey,
 target.PatientRenderingProviderKey = source.PatientRenderingProviderKey,
 target.PatientPrimaryProviderKey = source.PatientPrimaryProviderKey,
 target.PatientFirstName = TRIM(source.PatientFirstName),
 target.PatientLastName = TRIM(source.PatientLastName),
 target.PatientMiddleName = TRIM(source.PatientMiddleName),
 target.PatientPrefixName = TRIM(source.PatientPrefixName),
 target.PatientSuffixName = TRIM(source.PatientSuffixName),
 target.PatientFullName = TRIM(source.PatientFullName),
 target.PatientPrimaryAddressLine1 = TRIM(source.PatientPrimaryAddressLine1),
 target.PatientPrimaryAddressLine2 = TRIM(source.PatientPrimaryAddressLine2),
 target.PatientPrimaryGeographyKey = source.PatientPrimaryGeographyKey,
 target.PatientInitials = TRIM(source.PatientInitials),
 target.PatientSSN = TRIM(source.PatientSSN),
 target.PatientPrimaryServiceLocation = source.PatientPrimaryServiceLocation,
 target.PatientPhone = TRIM(source.PatientPhone),
 target.PatientMobilePhone = TRIM(source.PatientMobilePhone),
 target.PatientEmail = TRIM(source.PatientEmail),
 target.PatientRace = TRIM(source.PatientRace),
 target.PatientMaritalStatus = TRIM(source.PatientMaritalStatus),
 target.PatientLegacyMRN = TRIM(source.PatientLegacyMRN),
 target.PatientEthnicity = TRIM(source.PatientEthnicity),
 target.PatientSex = TRIM(source.PatientSex),
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
  INSERT (PatientKey, RegionKey, PatientDOBDateKey, PatientGuarantorPatientKey, PatientGuarantorID, PatientAttendingDoctorProviderKey, PatientReferringProviderKey, PatientRenderingProviderKey, PatientPrimaryProviderKey, PatientFirstName, PatientLastName, PatientMiddleName, PatientPrefixName, PatientSuffixName, PatientFullName, PatientPrimaryAddressLine1, PatientPrimaryAddressLine2, PatientPrimaryGeographyKey, PatientInitials, PatientSSN, PatientPrimaryServiceLocation, PatientPhone, PatientMobilePhone, PatientEmail, PatientRace, PatientMaritalStatus, PatientLegacyMRN, PatientEthnicity, PatientSex, DeleteFlag, SourceAPrimaryKeyValue, SourceARecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.PatientKey, source.RegionKey, source.PatientDOBDateKey, source.PatientGuarantorPatientKey, TRIM(source.PatientGuarantorID), source.PatientAttendingDoctorProviderKey, source.PatientReferringProviderKey, source.PatientRenderingProviderKey, source.PatientPrimaryProviderKey, TRIM(source.PatientFirstName), TRIM(source.PatientLastName), TRIM(source.PatientMiddleName), TRIM(source.PatientPrefixName), TRIM(source.PatientSuffixName), TRIM(source.PatientFullName), TRIM(source.PatientPrimaryAddressLine1), TRIM(source.PatientPrimaryAddressLine2), source.PatientPrimaryGeographyKey, TRIM(source.PatientInitials), TRIM(source.PatientSSN), source.PatientPrimaryServiceLocation, TRIM(source.PatientPhone), TRIM(source.PatientMobilePhone), TRIM(source.PatientEmail), TRIM(source.PatientRace), TRIM(source.PatientMaritalStatus), TRIM(source.PatientLegacyMRN), TRIM(source.PatientEthnicity), TRIM(source.PatientSex), source.DeleteFlag, TRIM(source.SourceAPrimaryKeyValue), source.SourceARecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PatientKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefPatientMeditechExpanse
      GROUP BY PatientKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefPatientMeditechExpanse');
ELSE
  COMMIT TRANSACTION;
END IF;
