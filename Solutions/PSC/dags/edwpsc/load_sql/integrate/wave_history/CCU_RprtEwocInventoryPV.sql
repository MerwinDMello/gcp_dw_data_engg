
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_RprtEwocInventoryPV AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_RprtEwocInventoryPV AS source
ON target.SnapShotDate = source.SnapShotDate AND target.PracticeName = source.PracticeName AND target.EncounterKey = source.EncounterKey
WHEN MATCHED THEN
  UPDATE SET
  target.SnapShotDate = source.SnapShotDate,
 target.Coid = TRIM(source.Coid),
 target.PracticeName = TRIM(source.PracticeName),
 target.EncounterKey = source.EncounterKey,
 target.EncounterLock = source.EncounterLock,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (SnapShotDate, Coid, PracticeName, EncounterKey, EncounterLock, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.SnapShotDate, TRIM(source.Coid), TRIM(source.PracticeName), source.EncounterKey, source.EncounterLock, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, PracticeName, EncounterKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_RprtEwocInventoryPV
      GROUP BY SnapShotDate, PracticeName, EncounterKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_RprtEwocInventoryPV');
ELSE
  COMMIT TRANSACTION;
END IF;
