
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterDiagnosisMeditechExpanse AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactEncounterDiagnosisMeditechExpanse AS source
ON target.EncounterDiagnosisMTXKey = source.EncounterDiagnosisMTXKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncounterDiagnosisMTXKey = source.EncounterDiagnosisMTXKey,
 target.RegionKey = source.RegionKey,
 target.EncounterKey = source.EncounterKey,
 target.DiagnosisCodeKey = source.DiagnosisCodeKey,
 target.DiagnosisCode = TRIM(source.DiagnosisCode),
 target.DiagnosisTransNum = source.DiagnosisTransNum,
 target.DiagnosisOrder = source.DiagnosisOrder,
 target.DiagnosisBchId = TRIM(source.DiagnosisBchId),
 target.DeleteFlag = source.DeleteFlag,
 target.SourceLastUpdatedDate = source.SourceLastUpdatedDate,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (EncounterDiagnosisMTXKey, RegionKey, EncounterKey, DiagnosisCodeKey, DiagnosisCode, DiagnosisTransNum, DiagnosisOrder, DiagnosisBchId, DeleteFlag, SourceLastUpdatedDate, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.EncounterDiagnosisMTXKey, source.RegionKey, source.EncounterKey, source.DiagnosisCodeKey, TRIM(source.DiagnosisCode), source.DiagnosisTransNum, source.DiagnosisOrder, TRIM(source.DiagnosisBchId), source.DeleteFlag, source.SourceLastUpdatedDate, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterDiagnosisMTXKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterDiagnosisMeditechExpanse
      GROUP BY EncounterDiagnosisMTXKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterDiagnosisMeditechExpanse');
ELSE
  COMMIT TRANSACTION;
END IF;
