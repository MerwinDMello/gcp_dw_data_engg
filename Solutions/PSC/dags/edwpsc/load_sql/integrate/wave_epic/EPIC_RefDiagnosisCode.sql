
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefDiagnosisCode AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefDiagnosisCode AS source
ON target.DiagnosisCodeKey = source.DiagnosisCodeKey
WHEN MATCHED THEN
  UPDATE SET
  target.DiagnosisCodeKey = source.DiagnosisCodeKey,
 target.DiagnosisCodeName = TRIM(source.DiagnosisCodeName),
 target.DiagnosisCode = TRIM(source.DiagnosisCode),
 target.DiagnosisICD10 = TRIM(source.DiagnosisICD10),
 target.DxId = TRIM(source.DxId),
 target.DiagnosisExternalId = TRIM(source.DiagnosisExternalId),
 target.DeleteFlag = source.DeleteFlag,
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKey = TRIM(source.SourceAPrimaryKey),
 target.SourceAPrimaryKeyLastUpdated = source.SourceAPrimaryKeyLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (DiagnosisCodeKey, DiagnosisCodeName, DiagnosisCode, DiagnosisICD10, DxId, DiagnosisExternalId, DeleteFlag, RegionKey, SourceAPrimaryKey, SourceAPrimaryKeyLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.DiagnosisCodeKey, TRIM(source.DiagnosisCodeName), TRIM(source.DiagnosisCode), TRIM(source.DiagnosisICD10), TRIM(source.DxId), TRIM(source.DiagnosisExternalId), source.DeleteFlag, source.RegionKey, TRIM(source.SourceAPrimaryKey), source.SourceAPrimaryKeyLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT DiagnosisCodeKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefDiagnosisCode
      GROUP BY DiagnosisCodeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefDiagnosisCode');
ELSE
  COMMIT TRANSACTION;
END IF;
