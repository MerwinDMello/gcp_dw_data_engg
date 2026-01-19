
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefDiagnosisCode AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefDiagnosisCode AS source
ON target.DiagnosisCodeKey = source.DiagnosisCodeKey
WHEN MATCHED THEN
  UPDATE SET
  target.DiagnosisCodeKey = source.DiagnosisCodeKey,
 target.DiagnosisCode = TRIM(source.DiagnosisCode),
 target.DiagnosisName = TRIM(source.DiagnosisName),
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBPrimaryKeyValue = source.SourceBPrimaryKeyValue,
 target.SourceBRecordLastUpdated = source.SourceBRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DeleteFlag = source.DeleteFlag
WHEN NOT MATCHED THEN
  INSERT (DiagnosisCodeKey, DiagnosisCode, DiagnosisName, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBPrimaryKeyValue, SourceBRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag)
  VALUES (source.DiagnosisCodeKey, TRIM(source.DiagnosisCode), TRIM(source.DiagnosisName), source.SourceAPrimaryKeyValue, source.SourceARecordLastUpdated, source.SourceBPrimaryKeyValue, source.SourceBRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT DiagnosisCodeKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefDiagnosisCode
      GROUP BY DiagnosisCodeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefDiagnosisCode');
ELSE
  COMMIT TRANSACTION;
END IF;
