
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactEncounterDiagnosis AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactEncounterDiagnosis AS source
ON target.EncounterDiagnosisKey = source.EncounterDiagnosisKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncounterDiagnosisKey = source.EncounterDiagnosisKey,
 target.RegionKey = source.RegionKey,
 target.EncounterKey = source.EncounterKey,
 target.EncounterId = source.EncounterId,
 target.PatientKey = source.PatientKey,
 target.PatientId = source.PatientId,
 target.DiagnosisCodeKey = source.DiagnosisCodeKey,
 target.DiagnosisCode = TRIM(source.DiagnosisCode),
 target.DxId = TRIM(source.DxId),
 target.PrimaryDxFlag = source.PrimaryDxFlag,
 target.Annotation = TRIM(source.Annotation),
 target.Qualifier = TRIM(source.Qualifier),
 target.Comments = TRIM(source.Comments),
 target.ChronicDxFlag = source.ChronicDxFlag,
 target.EncounterDate = source.EncounterDate,
 target.DiagnosisUpdateDate = source.DiagnosisUpdateDate,
 target.UniqueLineId = source.UniqueLineId,
 target.DxEditFlag = source.DxEditFlag,
 target.LinkedProblemId = source.LinkedProblemId,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceBPrimaryKeyValue = source.SourceBPrimaryKeyValue,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (EncounterDiagnosisKey, RegionKey, EncounterKey, EncounterId, PatientKey, PatientId, DiagnosisCodeKey, DiagnosisCode, DxId, PrimaryDxFlag, Annotation, Qualifier, Comments, ChronicDxFlag, EncounterDate, DiagnosisUpdateDate, UniqueLineId, DxEditFlag, LinkedProblemId, DWLastUpdateDateTime, SourceAPrimaryKeyValue, SourceBPrimaryKeyValue, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.EncounterDiagnosisKey, source.RegionKey, source.EncounterKey, source.EncounterId, source.PatientKey, source.PatientId, source.DiagnosisCodeKey, TRIM(source.DiagnosisCode), TRIM(source.DxId), source.PrimaryDxFlag, TRIM(source.Annotation), TRIM(source.Qualifier), TRIM(source.Comments), source.ChronicDxFlag, source.EncounterDate, source.DiagnosisUpdateDate, source.UniqueLineId, source.DxEditFlag, source.LinkedProblemId, source.DWLastUpdateDateTime, source.SourceAPrimaryKeyValue, source.SourceBPrimaryKeyValue, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterDiagnosisKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactEncounterDiagnosis
      GROUP BY EncounterDiagnosisKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactEncounterDiagnosis');
ELSE
  COMMIT TRANSACTION;
END IF;
