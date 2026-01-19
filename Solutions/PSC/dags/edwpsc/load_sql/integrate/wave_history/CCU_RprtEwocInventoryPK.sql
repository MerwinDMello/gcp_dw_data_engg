
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_RprtEwocInventoryPK AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_RprtEwocInventoryPK AS source
ON target.SnapShotDate = source.SnapShotDate
WHEN MATCHED THEN
  UPDATE SET
  target.SnapShotDate = source.SnapShotDate,
 target.SourceSystem = TRIM(source.SourceSystem),
 target.Coid = TRIM(source.Coid),
 target.OWNER = TRIM(source.OWNER),
 target.InventoryType = TRIM(source.InventoryType),
 target.EncounterLock = TRIM(source.EncounterLock),
 target.POSKey = TRIM(source.POSKey),
 target.EncounterKey = source.EncounterKey,
 target.BusinessDaysSinceServiceDate = source.BusinessDaysSinceServiceDate,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (SnapShotDate, SourceSystem, Coid, OWNER, InventoryType, EncounterLock, POSKey, EncounterKey, BusinessDaysSinceServiceDate, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.SnapShotDate, TRIM(source.SourceSystem), TRIM(source.Coid), TRIM(source.OWNER), TRIM(source.InventoryType), TRIM(source.EncounterLock), TRIM(source.POSKey), source.EncounterKey, source.BusinessDaysSinceServiceDate, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate
      FROM {{ params.param_psc_core_dataset_name }}.CCU_RprtEwocInventoryPK
      GROUP BY SnapShotDate
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_RprtEwocInventoryPK');
ELSE
  COMMIT TRANSACTION;
END IF;
