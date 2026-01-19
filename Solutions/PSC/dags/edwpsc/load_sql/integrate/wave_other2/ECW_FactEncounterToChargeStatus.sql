
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterToChargeStatus AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactEncounterToChargeStatus AS source
ON target.EncounterToChargeStatusKey = source.EncounterToChargeStatusKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncounterToChargeStatusKey = source.EncounterToChargeStatusKey,
 target.EncounterToChargeKey = source.EncounterToChargeKey,
 target.SourceSystem = TRIM(source.SourceSystem),
 target.SystemStatus = TRIM(source.SystemStatus),
 target.SystemStartDTM = source.SystemStartDTM,
 target.SystemStatusStartDTM = source.SystemStatusStartDTM,
 target.SystemCOID = TRIM(source.SystemCOID),
 target.OWNER = TRIM(source.OWNER),
 target.SourcePrimaryKeyValue = TRIM(source.SourcePrimaryKeyValue),
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (EncounterToChargeStatusKey, EncounterToChargeKey, SourceSystem, SystemStatus, SystemStartDTM, SystemStatusStartDTM, SystemCOID, OWNER, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.EncounterToChargeStatusKey, source.EncounterToChargeKey, TRIM(source.SourceSystem), TRIM(source.SystemStatus), source.SystemStartDTM, source.SystemStatusStartDTM, TRIM(source.SystemCOID), TRIM(source.OWNER), TRIM(source.SourcePrimaryKeyValue), source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterToChargeStatusKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterToChargeStatus
      GROUP BY EncounterToChargeStatusKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterToChargeStatus');
ELSE
  COMMIT TRANSACTION;
END IF;
