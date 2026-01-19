
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_FactEncounterModifier AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_FactEncounterModifier AS source
ON target.CCUEncounterModifierKey = source.CCUEncounterModifierKey
WHEN MATCHED THEN
  UPDATE SET
  target.Id = source.Id,
 target.EncounterId = source.EncounterId,
 target.InvoiceId = source.InvoiceId,
 target.patientID = source.patientID,
 target.displayIndex = source.displayIndex,
 target.itemID = source.itemID,
 target.MedDesc = TRIM(source.MedDesc),
 target.date = source.date,
 target.deleteFlag = source.deleteFlag,
 target.EncMod1 = TRIM(source.EncMod1),
 target.EncMod2 = TRIM(source.EncMod2),
 target.EncMod3 = TRIM(source.EncMod3),
 target.EncMod4 = TRIM(source.EncMod4),
 target.Created = source.Created,
 target.Modified = source.Modified,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.RegionKey = source.RegionKey,
 target.CCUEncounterModifierKey = source.CCUEncounterModifierKey
WHEN NOT MATCHED THEN
  INSERT (Id, EncounterId, InvoiceId, patientID, displayIndex, itemID, MedDesc, date, deleteFlag, EncMod1, EncMod2, EncMod3, EncMod4, Created, Modified, DWLastUpdateDateTime, RegionKey, CCUEncounterModifierKey)
  VALUES (source.Id, source.EncounterId, source.InvoiceId, source.patientID, source.displayIndex, source.itemID, TRIM(source.MedDesc), source.date, source.deleteFlag, TRIM(source.EncMod1), TRIM(source.EncMod2), TRIM(source.EncMod3), TRIM(source.EncMod4), source.Created, source.Modified, source.DWLastUpdateDateTime, source.RegionKey, source.CCUEncounterModifierKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CCUEncounterModifierKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_FactEncounterModifier
      GROUP BY CCUEncounterModifierKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_FactEncounterModifier');
ELSE
  COMMIT TRANSACTION;
END IF;
