
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_RefPatient_History AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_RefPatient_History AS source
ON target.PatientKey = source.PatientKey AND target.SysStartTime = source.SysStartTime AND target.SysEndTime = source.SysEndTime
WHEN MATCHED THEN
  UPDATE SET
  target.PatientKey = source.PatientKey,
 target.PatientDOBDateKey = source.PatientDOBDateKey,
 target.PatientAttendingDoctorProviderKey = source.PatientAttendingDoctorProviderKey,
 target.PatientGuarantorPatientKey = source.PatientGuarantorPatientKey,
 target.PatientGuarantorID = source.PatientGuarantorID,
 target.PatientStatementDateKey = source.PatientStatementDateKey,
 target.PatientReferringProviderKey = source.PatientReferringProviderKey,
 target.PatientRenderingProviderKey = source.PatientRenderingProviderKey,
 target.PatientPrimaryProviderKey = source.PatientPrimaryProviderKey,
 target.PatientDeceasedDateKey = TRIM(source.PatientDeceasedDateKey),
 target.PatientRelatedInfoDateKey = TRIM(source.PatientRelatedInfoDateKey),
 target.PatientFirstName = TRIM(source.PatientFirstName),
 target.PatientLastName = TRIM(source.PatientLastName),
 target.PatientMiddleName = TRIM(source.PatientMiddleName),
 target.PatientAccountNumberWithPrefix = TRIM(source.PatientAccountNumberWithPrefix),
 target.PatientPrefixName = TRIM(source.PatientPrefixName),
 target.PatientSuffixName = TRIM(source.PatientSuffixName),
 target.PatientPrimaryAddressLine1 = TRIM(source.PatientPrimaryAddressLine1),
 target.PatientPrimaryAddressLine2 = TRIM(source.PatientPrimaryAddressLine2),
 target.PatientPrimaryGeographyKey = source.PatientPrimaryGeographyKey,
 target.PatientUserType = source.PatientUserType,
 target.PatientInitials = TRIM(source.PatientInitials),
 target.PatientSSN = TRIM(source.PatientSSN),
 target.PatientPrimaryServiceLocation = source.PatientPrimaryServiceLocation,
 target.PatientPhone = TRIM(source.PatientPhone),
 target.PatientMobilePhone = TRIM(source.PatientMobilePhone),
 target.PatientPager = TRIM(source.PatientPager),
 target.PatientEmail = TRIM(source.PatientEmail),
 target.PatientStatus = TRIM(source.PatientStatus),
 target.PatientEmployerName = TRIM(source.PatientEmployerName),
 target.PatientEmployerAddressLine1 = TRIM(source.PatientEmployerAddressLine1),
 target.PatientEmployerAddressLine2 = TRIM(source.PatientEmployerAddressLine2),
 target.PatientEmployerGeographyKey = source.PatientEmployerGeographyKey,
 target.PatientEmployerPhone = TRIM(source.PatientEmployerPhone),
 target.PatientStudentStatus = TRIM(source.PatientStudentStatus),
 target.PatientEmployerStatus = TRIM(source.PatientEmployerStatus),
 target.PatientNoStatements = source.PatientNoStatements,
 target.PatientBillingAlert = source.PatientBillingAlert,
 target.PatientRace = TRIM(source.PatientRace),
 target.PatientMaritalStatus = TRIM(source.PatientMaritalStatus),
 target.PatientDeceased = source.PatientDeceased,
 target.PatientDeceasedNotes = TRIM(source.PatientDeceasedNotes),
 target.PatientLegacyMRN = TRIM(source.PatientLegacyMRN),
 target.PatientEthnicity = TRIM(source.PatientEthnicity),
 target.PatientRelatedInfo = TRIM(source.PatientRelatedInfo),
 target.PatientPharmacyName = TRIM(source.PatientPharmacyName),
 target.PatientPharmacyNo = TRIM(source.PatientPharmacyNo),
 target.PatientPharmacyFax = TRIM(source.PatientPharmacyFax),
 target.PatientCountryCode = TRIM(source.PatientCountryCode),
 target.PatientSex = TRIM(source.PatientSex),
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.PracticeKey = source.PracticeKey,
 target.PracticeName = TRIM(source.PracticeName),
 target.RegionKey = source.RegionKey,
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DeleteFlag = source.DeleteFlag,
 target.SourceBPrimaryKeyValue = source.SourceBPrimaryKeyValue,
 target.SourceBRecordLastUpdated = source.SourceBRecordLastUpdated,
 target.PatientHearFrom = TRIM(source.PatientHearFrom),
 target.PatientEmployerNumber = TRIM(source.PatientEmployerNumber),
 target.PatientEmployerNotes = TRIM(source.PatientEmployerNotes),
 target.PatientLanguage = TRIM(source.PatientLanguage),
 target.PatientPractice = TRIM(source.PatientPractice),
 target.PatientSourceGuid = TRIM(source.PatientSourceGuid),
 target.PatientSourcePrimaryProviderGuid = TRIM(source.PatientSourcePrimaryProviderGuid),
 target.EMPINumber = TRIM(source.EMPINumber),
 target.SysStartTime = source.SysStartTime,
 target.SysEndTime = source.SysEndTime
WHEN NOT MATCHED THEN
  INSERT (PatientKey, PatientDOBDateKey, PatientAttendingDoctorProviderKey, PatientGuarantorPatientKey, PatientGuarantorID, PatientStatementDateKey, PatientReferringProviderKey, PatientRenderingProviderKey, PatientPrimaryProviderKey, PatientDeceasedDateKey, PatientRelatedInfoDateKey, PatientFirstName, PatientLastName, PatientMiddleName, PatientAccountNumberWithPrefix, PatientPrefixName, PatientSuffixName, PatientPrimaryAddressLine1, PatientPrimaryAddressLine2, PatientPrimaryGeographyKey, PatientUserType, PatientInitials, PatientSSN, PatientPrimaryServiceLocation, PatientPhone, PatientMobilePhone, PatientPager, PatientEmail, PatientStatus, PatientEmployerName, PatientEmployerAddressLine1, PatientEmployerAddressLine2, PatientEmployerGeographyKey, PatientEmployerPhone, PatientStudentStatus, PatientEmployerStatus, PatientNoStatements, PatientBillingAlert, PatientRace, PatientMaritalStatus, PatientDeceased, PatientDeceasedNotes, PatientLegacyMRN, PatientEthnicity, PatientRelatedInfo, PatientPharmacyName, PatientPharmacyNo, PatientPharmacyFax, PatientCountryCode, PatientSex, SourceAPrimaryKeyValue, PracticeKey, PracticeName, RegionKey, SourceARecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag, SourceBPrimaryKeyValue, SourceBRecordLastUpdated, PatientHearFrom, PatientEmployerNumber, PatientEmployerNotes, PatientLanguage, PatientPractice, PatientSourceGuid, PatientSourcePrimaryProviderGuid, EMPINumber, SysStartTime, SysEndTime)
  VALUES (source.PatientKey, source.PatientDOBDateKey, source.PatientAttendingDoctorProviderKey, source.PatientGuarantorPatientKey, source.PatientGuarantorID, source.PatientStatementDateKey, source.PatientReferringProviderKey, source.PatientRenderingProviderKey, source.PatientPrimaryProviderKey, TRIM(source.PatientDeceasedDateKey), TRIM(source.PatientRelatedInfoDateKey), TRIM(source.PatientFirstName), TRIM(source.PatientLastName), TRIM(source.PatientMiddleName), TRIM(source.PatientAccountNumberWithPrefix), TRIM(source.PatientPrefixName), TRIM(source.PatientSuffixName), TRIM(source.PatientPrimaryAddressLine1), TRIM(source.PatientPrimaryAddressLine2), source.PatientPrimaryGeographyKey, source.PatientUserType, TRIM(source.PatientInitials), TRIM(source.PatientSSN), source.PatientPrimaryServiceLocation, TRIM(source.PatientPhone), TRIM(source.PatientMobilePhone), TRIM(source.PatientPager), TRIM(source.PatientEmail), TRIM(source.PatientStatus), TRIM(source.PatientEmployerName), TRIM(source.PatientEmployerAddressLine1), TRIM(source.PatientEmployerAddressLine2), source.PatientEmployerGeographyKey, TRIM(source.PatientEmployerPhone), TRIM(source.PatientStudentStatus), TRIM(source.PatientEmployerStatus), source.PatientNoStatements, source.PatientBillingAlert, TRIM(source.PatientRace), TRIM(source.PatientMaritalStatus), source.PatientDeceased, TRIM(source.PatientDeceasedNotes), TRIM(source.PatientLegacyMRN), TRIM(source.PatientEthnicity), TRIM(source.PatientRelatedInfo), TRIM(source.PatientPharmacyName), TRIM(source.PatientPharmacyNo), TRIM(source.PatientPharmacyFax), TRIM(source.PatientCountryCode), TRIM(source.PatientSex), source.SourceAPrimaryKeyValue, source.PracticeKey, TRIM(source.PracticeName), source.RegionKey, source.SourceARecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag, source.SourceBPrimaryKeyValue, source.SourceBRecordLastUpdated, TRIM(source.PatientHearFrom), TRIM(source.PatientEmployerNumber), TRIM(source.PatientEmployerNotes), TRIM(source.PatientLanguage), TRIM(source.PatientPractice), TRIM(source.PatientSourceGuid), TRIM(source.PatientSourcePrimaryProviderGuid), TRIM(source.EMPINumber), source.SysStartTime, source.SysEndTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PatientKey, SysStartTime, SysEndTime
      FROM {{ params.param_psc_core_dataset_name }}.PV_RefPatient_History
      GROUP BY PatientKey, SysStartTime, SysEndTime
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_RefPatient_History');
ELSE
  COMMIT TRANSACTION;
END IF;
