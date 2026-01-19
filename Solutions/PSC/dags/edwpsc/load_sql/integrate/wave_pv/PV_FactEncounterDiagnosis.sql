
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactEncounterDiagnosis AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactEncounterDiagnosis AS source
ON target.EncounterDiagnosisKey = source.EncounterDiagnosisKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncounterDiagnosisKey = source.EncounterDiagnosisKey,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.PracticeName = TRIM(source.PracticeName),
 target.EncounterKey = source.EncounterKey,
 target.EncounterID = source.EncounterID,
 target.DiagnosisCodeKey = source.DiagnosisCodeKey,
 target.ICD10 = TRIM(source.ICD10),
 target.ICD9 = TRIM(source.ICD9),
 target.ICDOrder = source.ICDOrder,
 target.VisitDate = source.VisitDate,
 target.DeleteFlag = source.DeleteFlag,
 target.SourcePrimaryKeyValue = TRIM(source.SourcePrimaryKeyValue),
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (EncounterDiagnosisKey, RegionKey, Coid, PracticeName, EncounterKey, EncounterID, DiagnosisCodeKey, ICD10, ICD9, ICDOrder, VisitDate, DeleteFlag, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.EncounterDiagnosisKey, source.RegionKey, TRIM(source.Coid), TRIM(source.PracticeName), source.EncounterKey, source.EncounterID, source.DiagnosisCodeKey, TRIM(source.ICD10), TRIM(source.ICD9), source.ICDOrder, source.VisitDate, source.DeleteFlag, TRIM(source.SourcePrimaryKeyValue), source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterDiagnosisKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactEncounterDiagnosis
      GROUP BY EncounterDiagnosisKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactEncounterDiagnosis');
ELSE
  COMMIT TRANSACTION;
END IF;
