
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefPractice AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefPractice AS source
ON target.PracticeKey = source.PracticeKey
WHEN MATCHED THEN
  UPDATE SET
  target.PracticeKey = source.PracticeKey,
 target.PracticeName = TRIM(source.PracticeName),
 target.PracticeAbbr = TRIM(source.PracticeAbbr),
 target.PracticeSpecialty = TRIM(source.PracticeSpecialty),
 target.PracticeRevLocId = TRIM(source.PracticeRevLocId),
 target.PracticeGroup = source.PracticeGroup,
 target.PracticeGlPrefix = TRIM(source.PracticeGlPrefix),
 target.PracticeAdtParentId = TRIM(source.PracticeAdtParentId),
 target.PracticeServAreaId = TRIM(source.PracticeServAreaId),
 target.PracticeServiceAreaKey = source.PracticeServiceAreaKey,
 target.PracticeSpecialtyDept = TRIM(source.PracticeSpecialtyDept),
 target.PracticeCenter = TRIM(source.PracticeCenter),
 target.PracticeActiveFlag = TRIM(source.PracticeActiveFlag),
 target.PracticeExternalName = TRIM(source.PracticeExternalName),
 target.PracticePhone = TRIM(source.PracticePhone),
 target.PracticeFax = TRIM(source.PracticeFax),
 target.PracticeFacility = TRIM(source.PracticeFacility),
 target.PracticeAddress1 = TRIM(source.PracticeAddress1),
 target.PracticeAddress2 = TRIM(source.PracticeAddress2),
 target.PracticeCity = TRIM(source.PracticeCity),
 target.PracticeState = TRIM(source.PracticeState),
 target.PracticeZip = TRIM(source.PracticeZip),
 target.PracticeEmergencyPhone = TRIM(source.PracticeEmergencyPhone),
 target.DepartmentId = TRIM(source.DepartmentId),
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKey = TRIM(source.SourceAPrimaryKey),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.PracticePrimaryGeographyKey = source.PracticePrimaryGeographyKey,
 target.PracticeFederalTaxID = TRIM(source.PracticeFederalTaxID),
 target.PracticeGroupNPI = TRIM(source.PracticeGroupNPI),
 target.InternalDepartmentId = source.InternalDepartmentId
WHEN NOT MATCHED THEN
  INSERT (PracticeKey, PracticeName, PracticeAbbr, PracticeSpecialty, PracticeRevLocId, PracticeGroup, PracticeGlPrefix, PracticeAdtParentId, PracticeServAreaId, PracticeServiceAreaKey, PracticeSpecialtyDept, PracticeCenter, PracticeActiveFlag, PracticeExternalName, PracticePhone, PracticeFax, PracticeFacility, PracticeAddress1, PracticeAddress2, PracticeCity, PracticeState, PracticeZip, PracticeEmergencyPhone, DepartmentId, RegionKey, SourceAPrimaryKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, PracticePrimaryGeographyKey, PracticeFederalTaxID, PracticeGroupNPI, InternalDepartmentId)
  VALUES (source.PracticeKey, TRIM(source.PracticeName), TRIM(source.PracticeAbbr), TRIM(source.PracticeSpecialty), TRIM(source.PracticeRevLocId), source.PracticeGroup, TRIM(source.PracticeGlPrefix), TRIM(source.PracticeAdtParentId), TRIM(source.PracticeServAreaId), source.PracticeServiceAreaKey, TRIM(source.PracticeSpecialtyDept), TRIM(source.PracticeCenter), TRIM(source.PracticeActiveFlag), TRIM(source.PracticeExternalName), TRIM(source.PracticePhone), TRIM(source.PracticeFax), TRIM(source.PracticeFacility), TRIM(source.PracticeAddress1), TRIM(source.PracticeAddress2), TRIM(source.PracticeCity), TRIM(source.PracticeState), TRIM(source.PracticeZip), TRIM(source.PracticeEmergencyPhone), TRIM(source.DepartmentId), source.RegionKey, TRIM(source.SourceAPrimaryKey), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.PracticePrimaryGeographyKey, TRIM(source.PracticeFederalTaxID), TRIM(source.PracticeGroupNPI), source.InternalDepartmentId);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PracticeKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefPractice
      GROUP BY PracticeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefPractice');
ELSE
  COMMIT TRANSACTION;
END IF;
