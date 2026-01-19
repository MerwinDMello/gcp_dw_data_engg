
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefEncounterActionLogType AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefEncounterActionLogType AS source
ON target.EncounterActionLogTypeKey = source.EncounterActionLogTypeKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncounterActionLogTypeKey = source.EncounterActionLogTypeKey,
 target.EncounterActionLogTypeCode = source.EncounterActionLogTypeCode,
 target.EncounterActionLogTypeName = TRIM(source.EncounterActionLogTypeName),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (EncounterActionLogTypeKey, EncounterActionLogTypeCode, EncounterActionLogTypeName, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.EncounterActionLogTypeKey, source.EncounterActionLogTypeCode, TRIM(source.EncounterActionLogTypeName), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterActionLogTypeKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefEncounterActionLogType
      GROUP BY EncounterActionLogTypeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefEncounterActionLogType');
ELSE
  COMMIT TRANSACTION;
END IF;
