
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterMedaxionCodingCptCodes AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactEncounterMedaxionCodingCptCodes AS source
ON target.MedaxionCodingCptCodesKey = source.MedaxionCodingCptCodesKey
WHEN MATCHED THEN
  UPDATE SET
  target.MedaxionCodingCptCodesKey = source.MedaxionCodingCptCodesKey,
 target.MedaxionCodingStatusKey = source.MedaxionCodingStatusKey,
 target.CptCode = TRIM(source.CptCode),
 target.ModifierCodes = TRIM(source.ModifierCodes),
 target.DiagnosisCodes = TRIM(source.DiagnosisCodes),
 target.CptProviderName = TRIM(source.CptProviderName),
 target.CptCount = source.CptCount,
 target.DeleteFlag = source.DeleteFlag,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (MedaxionCodingCptCodesKey, MedaxionCodingStatusKey, CptCode, ModifierCodes, DiagnosisCodes, CptProviderName, CptCount, DeleteFlag, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.MedaxionCodingCptCodesKey, source.MedaxionCodingStatusKey, TRIM(source.CptCode), TRIM(source.ModifierCodes), TRIM(source.DiagnosisCodes), TRIM(source.CptProviderName), source.CptCount, source.DeleteFlag, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT MedaxionCodingCptCodesKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterMedaxionCodingCptCodes
      GROUP BY MedaxionCodingCptCodesKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterMedaxionCodingCptCodes');
ELSE
  COMMIT TRANSACTION;
END IF;
