
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefProviderMasterListMedicaid AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefProviderMasterListMedicaid AS source
ON target.ProviderMasterListMedicaidKey = source.ProviderMasterListMedicaidKey
WHEN MATCHED THEN
  UPDATE SET
  target.ProviderMasterListMedicaidKey = source.ProviderMasterListMedicaidKey,
 target.MedicaidProviderID = TRIM(source.MedicaidProviderID),
 target.SourceSystem = TRIM(source.SourceSystem),
 target.ProviderName = TRIM(source.ProviderName),
 target.DBAName = TRIM(source.DBAName),
 target.ProviderTypeCode = TRIM(source.ProviderTypeCode),
 target.ProviderSpecialtyCode = TRIM(source.ProviderSpecialtyCode),
 target.TaxonomyCode = TRIM(source.TaxonomyCode),
 target.ServiceLocationAddress1 = TRIM(source.ServiceLocationAddress1),
 target.ServiceLocationAddress2 = TRIM(source.ServiceLocationAddress2),
 target.ServiceLocationAddressCity = TRIM(source.ServiceLocationAddressCity),
 target.ServiceLocationAddressState = TRIM(source.ServiceLocationAddressState),
 target.ServiceLocationAddresszip = TRIM(source.ServiceLocationAddresszip),
 target.EnrollmentType = TRIM(source.EnrollmentType),
 target.NPIType = TRIM(source.NPIType),
 target.NPI = TRIM(source.NPI),
 target.NPIEffectiveDate = source.NPIEffectiveDate,
 target.NPIEndDate = source.NPIEndDate,
 target.NPIStatus = TRIM(source.NPIStatus),
 target.IndividualorOrganizational = TRIM(source.IndividualorOrganizational),
 target.License = TRIM(source.License),
 target.CurrentMedicaidEnrollmentStatus = TRIM(source.CurrentMedicaidEnrollmentStatus),
 target.MedicaidClaimsEligibilityEffectiveDate = source.MedicaidClaimsEligibilityEffectiveDate,
 target.MedicaidClaimsEligibilityEndDate = source.MedicaidClaimsEligibilityEndDate,
 target.NextRevalidationDate = source.NextRevalidationDate,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.FileName = TRIM(source.FileName),
 target.FileDate = TRIM(source.FileDate)
WHEN NOT MATCHED THEN
  INSERT (ProviderMasterListMedicaidKey, MedicaidProviderID, SourceSystem, ProviderName, DBAName, ProviderTypeCode, ProviderSpecialtyCode, TaxonomyCode, ServiceLocationAddress1, ServiceLocationAddress2, ServiceLocationAddressCity, ServiceLocationAddressState, ServiceLocationAddresszip, EnrollmentType, NPIType, NPI, NPIEffectiveDate, NPIEndDate, NPIStatus, IndividualorOrganizational, License, CurrentMedicaidEnrollmentStatus, MedicaidClaimsEligibilityEffectiveDate, MedicaidClaimsEligibilityEndDate, NextRevalidationDate, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, FileName, FileDate)
  VALUES (source.ProviderMasterListMedicaidKey, TRIM(source.MedicaidProviderID), TRIM(source.SourceSystem), TRIM(source.ProviderName), TRIM(source.DBAName), TRIM(source.ProviderTypeCode), TRIM(source.ProviderSpecialtyCode), TRIM(source.TaxonomyCode), TRIM(source.ServiceLocationAddress1), TRIM(source.ServiceLocationAddress2), TRIM(source.ServiceLocationAddressCity), TRIM(source.ServiceLocationAddressState), TRIM(source.ServiceLocationAddresszip), TRIM(source.EnrollmentType), TRIM(source.NPIType), TRIM(source.NPI), source.NPIEffectiveDate, source.NPIEndDate, TRIM(source.NPIStatus), TRIM(source.IndividualorOrganizational), TRIM(source.License), TRIM(source.CurrentMedicaidEnrollmentStatus), source.MedicaidClaimsEligibilityEffectiveDate, source.MedicaidClaimsEligibilityEndDate, source.NextRevalidationDate, source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.FileName), TRIM(source.FileDate));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ProviderMasterListMedicaidKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefProviderMasterListMedicaid
      GROUP BY ProviderMasterListMedicaidKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefProviderMasterListMedicaid');
ELSE
  COMMIT TRANSACTION;
END IF;
