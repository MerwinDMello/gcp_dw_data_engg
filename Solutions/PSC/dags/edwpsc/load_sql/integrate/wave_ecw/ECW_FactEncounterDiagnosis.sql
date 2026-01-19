
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterDiagnosis AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactEncounterDiagnosis AS source
ON target.EncounterDiagnosisKey = source.EncounterDiagnosisKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncounterDiagnosisKey = source.EncounterDiagnosisKey,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.EncounterKey = source.EncounterKey,
 target.EncounterID = source.EncounterID,
 target.DiagnosisCodeKey = source.DiagnosisCodeKey,
 target.ICDCode = TRIM(source.ICDCode),
 target.ICDOrder = source.ICDOrder,
 target.ICDType = source.ICDType,
 target.ICDDescription = TRIM(source.ICDDescription),
 target.VisitDate = source.VisitDate,
 target.PrimaryFlag = source.PrimaryFlag,
 target.DeleteFlag = source.DeleteFlag,
 target.Specify = TRIM(source.Specify),
 target.Notes = TRIM(source.Notes),
 target.AssessmentOnsetDate = source.AssessmentOnsetDate,
 target.SnoMed = TRIM(source.SnoMed),
 target.SnoMedDescription = TRIM(source.SnoMedDescription),
 target.SynonymId = source.SynonymId,
 target.Axis = source.Axis,
 target.SourcePrimaryKeyValue = TRIM(source.SourcePrimaryKeyValue),
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.ArchivedRecord = TRIM(source.ArchivedRecord)
WHEN NOT MATCHED THEN
  INSERT (EncounterDiagnosisKey, RegionKey, Coid, EncounterKey, EncounterID, DiagnosisCodeKey, ICDCode, ICDOrder, ICDType, ICDDescription, VisitDate, PrimaryFlag, DeleteFlag, Specify, Notes, AssessmentOnsetDate, SnoMed, SnoMedDescription, SynonymId, Axis, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ArchivedRecord)
  VALUES (source.EncounterDiagnosisKey, source.RegionKey, TRIM(source.Coid), source.EncounterKey, source.EncounterID, source.DiagnosisCodeKey, TRIM(source.ICDCode), source.ICDOrder, source.ICDType, TRIM(source.ICDDescription), source.VisitDate, source.PrimaryFlag, source.DeleteFlag, TRIM(source.Specify), TRIM(source.Notes), source.AssessmentOnsetDate, TRIM(source.SnoMed), TRIM(source.SnoMedDescription), source.SynonymId, source.Axis, TRIM(source.SourcePrimaryKeyValue), source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.ArchivedRecord));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterDiagnosisKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterDiagnosis
      GROUP BY EncounterDiagnosisKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterDiagnosis');
ELSE
  COMMIT TRANSACTION;
END IF;
