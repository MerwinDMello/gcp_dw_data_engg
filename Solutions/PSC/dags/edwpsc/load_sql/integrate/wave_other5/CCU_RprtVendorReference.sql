
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_RprtVendorReference AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_RprtVendorReference AS source
ON target.CCUVendorReferenceKey = source.CCUVendorReferenceKey
WHEN MATCHED THEN
  UPDATE SET
  target.CCUVendorReferenceKey = source.CCUVendorReferenceKey,
 target.`34id` = TRIM(source.`34id`),
 target.LastName = TRIM(source.LastName),
 target.FirstName = TRIM(source.FirstName),
 target.Vendor = TRIM(source.Vendor),
 target.ActiveStatus = TRIM(source.ActiveStatus),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.SharePointDateModified = source.SharePointDateModified,
 target.SharePointDateCreated = source.SharePointDateCreated,
 target.`Group` = TRIM(source.`Group`),
 target.Area = TRIM(source.Area),
 target.Directors = TRIM(source.Directors),
 target.Manager = TRIM(source.Manager),
 target.TermDate = source.TermDate,
 target.CoderStatus = TRIM(source.CoderStatus),
 target.Specialty = TRIM(source.Specialty),
 target.SubSpecialty = TRIM(source.SubSpecialty),
 target.HourlyBenchmark = source.HourlyBenchmark,
 target.Comments = TRIM(source.Comments),
 target.SharePointCreatedBy = TRIM(source.SharePointCreatedBy),
 target.SharePointModifiedBy = TRIM(source.SharePointModifiedBy)
WHEN NOT MATCHED THEN
  INSERT (CCUVendorReferenceKey, `34id`, LastName, FirstName, Vendor, ActiveStatus, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, SharePointDateModified, SharePointDateCreated, `Group`, Area, Directors, Manager, TermDate, CoderStatus, Specialty, SubSpecialty, HourlyBenchmark, Comments, SharePointCreatedBy, SharePointModifiedBy)
  VALUES (source.CCUVendorReferenceKey, TRIM(source.`34id`), TRIM(source.LastName), TRIM(source.FirstName), TRIM(source.Vendor), TRIM(source.ActiveStatus), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.SharePointDateModified, source.SharePointDateCreated, TRIM(source.`Group`), TRIM(source.Area), TRIM(source.Directors), TRIM(source.Manager), source.TermDate, TRIM(source.CoderStatus), TRIM(source.Specialty), TRIM(source.SubSpecialty), source.HourlyBenchmark, TRIM(source.Comments), TRIM(source.SharePointCreatedBy), TRIM(source.SharePointModifiedBy));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CCUVendorReferenceKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_RprtVendorReference
      GROUP BY CCUVendorReferenceKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_RprtVendorReference');
ELSE
  COMMIT TRANSACTION;
END IF;
