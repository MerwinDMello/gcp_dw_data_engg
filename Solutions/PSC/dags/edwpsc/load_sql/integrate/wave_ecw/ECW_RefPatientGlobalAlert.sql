
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefPatientGlobalAlert AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefPatientGlobalAlert AS source
ON target.PatientGlobalAlertKey = source.PatientGlobalAlertKey
WHEN MATCHED THEN
  UPDATE SET
  target.PatientGlobalAlertKey = source.PatientGlobalAlertKey,
 target.PatientKey = source.PatientKey,
 target.PatientGlobalAlertCodeKey = source.PatientGlobalAlertCodeKey,
 target.PatientGlobalAlertNote = TRIM(source.PatientGlobalAlertNote),
 target.PatientGlobalAlertPriority = source.PatientGlobalAlertPriority,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DeleteFlag = source.DeleteFlag,
 target.RegionKey = source.RegionKey
WHEN NOT MATCHED THEN
  INSERT (PatientGlobalAlertKey, PatientKey, PatientGlobalAlertCodeKey, PatientGlobalAlertNote, PatientGlobalAlertPriority, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag, RegionKey)
  VALUES (source.PatientGlobalAlertKey, source.PatientKey, source.PatientGlobalAlertCodeKey, TRIM(source.PatientGlobalAlertNote), source.PatientGlobalAlertPriority, source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag, source.RegionKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PatientGlobalAlertKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefPatientGlobalAlert
      GROUP BY PatientGlobalAlertKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefPatientGlobalAlert');
ELSE
  COMMIT TRANSACTION;
END IF;
