
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PK_FactDiagnosisCodes AS target
USING {{ params.param_psc_stage_dataset_name }}.PK_FactDiagnosisCodes AS source
ON target.PKDiagnosisCodeKey = source.PKDiagnosisCodeKey
WHEN MATCHED THEN
  UPDATE SET
  target.PKDiagnosisCodeKey = source.PKDiagnosisCodeKey,
 target.ICD10Code = TRIM(source.ICD10Code),
 target.ICD10CodeDescription = TRIM(source.ICD10CodeDescription),
 target.ICD10Order = source.ICD10Order,
 target.DeleteFlag = source.DeleteFlag,
 target.PKRegionName = TRIM(source.PKRegionName),
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceBPrimaryKeyValue = source.SourceBPrimaryKeyValue,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (PKDiagnosisCodeKey, ICD10Code, ICD10CodeDescription, ICD10Order, DeleteFlag, PKRegionName, SourceSystemCode, SourceAPrimaryKeyValue, SourceBPrimaryKeyValue, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.PKDiagnosisCodeKey, TRIM(source.ICD10Code), TRIM(source.ICD10CodeDescription), source.ICD10Order, source.DeleteFlag, TRIM(source.PKRegionName), TRIM(source.SourceSystemCode), source.SourceAPrimaryKeyValue, source.SourceBPrimaryKeyValue, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PKDiagnosisCodeKey
      FROM {{ params.param_psc_core_dataset_name }}.PK_FactDiagnosisCodes
      GROUP BY PKDiagnosisCodeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PK_FactDiagnosisCodes');
ELSE
  COMMIT TRANSACTION;
END IF;
