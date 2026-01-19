
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefDiagnosisCodeMeditechExpanse AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefDiagnosisCodeMeditechExpanse AS source
ON target.DiagnosisCodeKey = source.DiagnosisCodeKey
WHEN MATCHED THEN
  UPDATE SET
  target.DiagnosisCodeKey = source.DiagnosisCodeKey,
 target.DiagnosisCode = TRIM(source.DiagnosisCode),
 target.DiagnosisName = TRIM(source.DiagnosisName),
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
  INSERT (DiagnosisCodeKey, DiagnosisCode, DiagnosisName, DeleteFlag, SourceAPrimaryKeyValue, SourceARecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.DiagnosisCodeKey, TRIM(source.DiagnosisCode), TRIM(source.DiagnosisName), source.DeleteFlag, TRIM(source.SourceAPrimaryKeyValue), source.SourceARecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT DiagnosisCodeKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefDiagnosisCodeMeditechExpanse
      GROUP BY DiagnosisCodeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefDiagnosisCodeMeditechExpanse');
ELSE
  COMMIT TRANSACTION;
END IF;
