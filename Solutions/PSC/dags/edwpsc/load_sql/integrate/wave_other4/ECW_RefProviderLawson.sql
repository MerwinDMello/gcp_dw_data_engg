
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefProviderLawson AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefProviderLawson AS source
ON target.ProviderLawsonKey = source.ProviderLawsonKey
WHEN MATCHED THEN
  UPDATE SET
  target.ProviderLawsonKey = source.ProviderLawsonKey,
 target.ProviderLawsonEmployeeNumber = TRIM(source.ProviderLawsonEmployeeNumber),
 target.ProviderLawsonLastName = TRIM(source.ProviderLawsonLastName),
 target.ProviderLawsonFirstName = TRIM(source.ProviderLawsonFirstName),
 target.ProviderLawsonMiddleName = TRIM(source.ProviderLawsonMiddleName),
 target.ProviderLawsonEmpStatus = TRIM(source.ProviderLawsonEmpStatus),
 target.ProviderLawsonDepartment = TRIM(source.ProviderLawsonDepartment),
 target.ProviderLawsonJobCode = TRIM(source.ProviderLawsonJobCode),
 target.ProviderLawsonDescription = TRIM(source.ProviderLawsonDescription),
 target.ProviderLawsonAnniversHired = source.ProviderLawsonAnniversHired,
 target.ProviderLawsonNewHireDate = source.ProviderLawsonNewHireDate,
 target.ProviderLawsonPrmcertID = TRIM(source.ProviderLawsonPrmcertID),
 target.COID = TRIM(source.COID),
 target.ProviderLawsonUserId = TRIM(source.ProviderLawsonUserId),
 target.ProviderLawsonTerminationDate = source.ProviderLawsonTerminationDate,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBRecordLastUpdated = source.SourceBRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DeleteFlag = source.DeleteFlag
WHEN NOT MATCHED THEN
  INSERT (ProviderLawsonKey, ProviderLawsonEmployeeNumber, ProviderLawsonLastName, ProviderLawsonFirstName, ProviderLawsonMiddleName, ProviderLawsonEmpStatus, ProviderLawsonDepartment, ProviderLawsonJobCode, ProviderLawsonDescription, ProviderLawsonAnniversHired, ProviderLawsonNewHireDate, ProviderLawsonPrmcertID, COID, ProviderLawsonUserId, ProviderLawsonTerminationDate, SourcePrimaryKeyValue, SourceARecordLastUpdated, SourceBRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag)
  VALUES (source.ProviderLawsonKey, TRIM(source.ProviderLawsonEmployeeNumber), TRIM(source.ProviderLawsonLastName), TRIM(source.ProviderLawsonFirstName), TRIM(source.ProviderLawsonMiddleName), TRIM(source.ProviderLawsonEmpStatus), TRIM(source.ProviderLawsonDepartment), TRIM(source.ProviderLawsonJobCode), TRIM(source.ProviderLawsonDescription), source.ProviderLawsonAnniversHired, source.ProviderLawsonNewHireDate, TRIM(source.ProviderLawsonPrmcertID), TRIM(source.COID), TRIM(source.ProviderLawsonUserId), source.ProviderLawsonTerminationDate, source.SourcePrimaryKeyValue, source.SourceARecordLastUpdated, source.SourceBRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ProviderLawsonKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefProviderLawson
      GROUP BY ProviderLawsonKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefProviderLawson');
ELSE
  COMMIT TRANSACTION;
END IF;
