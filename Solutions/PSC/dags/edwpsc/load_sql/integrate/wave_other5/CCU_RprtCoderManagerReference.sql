
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_RprtCoderManagerReference AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_RprtCoderManagerReference AS source
ON target.CoderManagerReferenceKey = source.CoderManagerReferenceKey
WHEN MATCHED THEN
  UPDATE SET
  target.CoderManagerReferenceKey = source.CoderManagerReferenceKey,
 target.EmployeeNumber = TRIM(source.EmployeeNumber),
 target.`34id` = TRIM(source.`34id`),
 target.eCWUserName = TRIM(source.eCWUserName),
 target.SharepointUserName = TRIM(source.SharepointUserName),
 target.Employee = TRIM(source.Employee),
 target.ManagerII = TRIM(source.ManagerII),
 target.ManagerI = TRIM(source.ManagerI),
 target.ReportRequired = TRIM(source.ReportRequired),
 target.Specialty = TRIM(source.Specialty),
 target.HourlyBenchmark = source.HourlyBenchmark,
 target.Title = TRIM(source.Title),
 target.Status = TRIM(source.Status),
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
 target.SubSpecialty = TRIM(source.SubSpecialty),
 target.Comments = TRIM(source.Comments),
 target.SharePointCreatedBy = TRIM(source.SharePointCreatedBy),
 target.SharePointModifiedBy = TRIM(source.SharePointModifiedBy),
 target.LastName = TRIM(source.LastName),
 target.FirstName = TRIM(source.FirstName)
WHEN NOT MATCHED THEN
  INSERT (CoderManagerReferenceKey, EmployeeNumber, `34id`, eCWUserName, SharepointUserName, Employee, ManagerII, ManagerI, ReportRequired, Specialty, HourlyBenchmark, Title, Status, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, SharePointDateModified, SharePointDateCreated, `Group`, Area, Directors, Manager, TermDate, CoderStatus, SubSpecialty, Comments, SharePointCreatedBy, SharePointModifiedBy, LastName, FirstName)
  VALUES (source.CoderManagerReferenceKey, TRIM(source.EmployeeNumber), TRIM(source.`34id`), TRIM(source.eCWUserName), TRIM(source.SharepointUserName), TRIM(source.Employee), TRIM(source.ManagerII), TRIM(source.ManagerI), TRIM(source.ReportRequired), TRIM(source.Specialty), source.HourlyBenchmark, TRIM(source.Title), TRIM(source.Status), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.SharePointDateModified, source.SharePointDateCreated, TRIM(source.`Group`), TRIM(source.Area), TRIM(source.Directors), TRIM(source.Manager), source.TermDate, TRIM(source.CoderStatus), TRIM(source.SubSpecialty), TRIM(source.Comments), TRIM(source.SharePointCreatedBy), TRIM(source.SharePointModifiedBy), TRIM(source.LastName), TRIM(source.FirstName));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CoderManagerReferenceKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_RprtCoderManagerReference
      GROUP BY CoderManagerReferenceKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_RprtCoderManagerReference');
ELSE
  COMMIT TRANSACTION;
END IF;
