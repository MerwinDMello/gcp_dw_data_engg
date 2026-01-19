
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefPractice AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefPractice AS source
ON target.PracticeKey = source.PracticeKey
WHEN MATCHED THEN
  UPDATE SET
  target.PracticeKey = source.PracticeKey,
 target.PracticeName = TRIM(source.PracticeName),
 target.PracticePrimaryAddressLine1 = TRIM(source.PracticePrimaryAddressLine1),
 target.PracticePrimaryAddressLine2 = TRIM(source.PracticePrimaryAddressLine2),
 target.PracticePrimaryGeographyKey = source.PracticePrimaryGeographyKey,
 target.PracticeTelephone = TRIM(source.PracticeTelephone),
 target.PracticeFax = TRIM(source.PracticeFax),
 target.PracticeEmail = TRIM(source.PracticeEmail),
 target.PracticeSiteID = TRIM(source.PracticeSiteID),
 target.PracticeFederalTaxID = TRIM(source.PracticeFederalTaxID),
 target.PracticeCliaID = TRIM(source.PracticeCliaID),
 target.PracticeParentTIN = TRIM(source.PracticeParentTIN),
 target.PracticeGroupNPI = TRIM(source.PracticeGroupNPI),
 target.PracticeTaxonomyCode = TRIM(source.PracticeTaxonomyCode),
 target.PracticeLockboxNo = TRIM(source.PracticeLockboxNo),
 target.PracticeCOB = TRIM(source.PracticeCOB),
 target.PracticePrimaryCOID = TRIM(source.PracticePrimaryCOID),
 target.PracticeStartDateKey = source.PracticeStartDateKey,
 target.PracticeEndDateKey = source.PracticeEndDateKey,
 target.PracticeSpinDownDateKey = source.PracticeSpinDownDateKey,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
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
  INSERT (PracticeKey, PracticeName, PracticePrimaryAddressLine1, PracticePrimaryAddressLine2, PracticePrimaryGeographyKey, PracticeTelephone, PracticeFax, PracticeEmail, PracticeSiteID, PracticeFederalTaxID, PracticeCliaID, PracticeParentTIN, PracticeGroupNPI, PracticeTaxonomyCode, PracticeLockboxNo, PracticeCOB, PracticePrimaryCOID, PracticeStartDateKey, PracticeEndDateKey, PracticeSpinDownDateKey, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag, RegionKey, SysStartTime, SysEndTime)
  VALUES (source.PracticeKey, TRIM(source.PracticeName), TRIM(source.PracticePrimaryAddressLine1), TRIM(source.PracticePrimaryAddressLine2), source.PracticePrimaryGeographyKey, TRIM(source.PracticeTelephone), TRIM(source.PracticeFax), TRIM(source.PracticeEmail), TRIM(source.PracticeSiteID), TRIM(source.PracticeFederalTaxID), TRIM(source.PracticeCliaID), TRIM(source.PracticeParentTIN), TRIM(source.PracticeGroupNPI), TRIM(source.PracticeTaxonomyCode), TRIM(source.PracticeLockboxNo), TRIM(source.PracticeCOB), TRIM(source.PracticePrimaryCOID), source.PracticeStartDateKey, source.PracticeEndDateKey, source.PracticeSpinDownDateKey, source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag, source.RegionKey, source.SysStartTime, source.SysEndTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PracticeKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefPractice
      GROUP BY PracticeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefPractice');
ELSE
  COMMIT TRANSACTION;
END IF;
