
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefMedicareLocality AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefMedicareLocality AS source
ON target.MedicareLocalityKey = source.MedicareLocalityKey
WHEN MATCHED THEN
  UPDATE SET
  target.MedicareLocalityKey = source.MedicareLocalityKey,
 target.State = TRIM(source.State),
 target.ZipCode = TRIM(source.ZipCode),
 target.Carrier = TRIM(source.Carrier),
 target.Locality = TRIM(source.Locality),
 target.RuralInd = TRIM(source.RuralInd),
 target.LabCbLocality = TRIM(source.LabCbLocality),
 target.RuralInd2 = TRIM(source.RuralInd2),
 target.Plus4Flag = source.Plus4Flag,
 target.PartBDrugIndicator = TRIM(source.PartBDrugIndicator),
 target.YearQtr = source.YearQtr,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.MedicareLocality = TRIM(source.MedicareLocality)
WHEN NOT MATCHED THEN
  INSERT (MedicareLocalityKey, State, ZipCode, Carrier, Locality, RuralInd, LabCbLocality, RuralInd2, Plus4Flag, PartBDrugIndicator, YearQtr, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, MedicareLocality)
  VALUES (source.MedicareLocalityKey, TRIM(source.State), TRIM(source.ZipCode), TRIM(source.Carrier), TRIM(source.Locality), TRIM(source.RuralInd), TRIM(source.LabCbLocality), TRIM(source.RuralInd2), source.Plus4Flag, TRIM(source.PartBDrugIndicator), source.YearQtr, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.MedicareLocality));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT MedicareLocalityKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefMedicareLocality
      GROUP BY MedicareLocalityKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefMedicareLocality');
ELSE
  COMMIT TRANSACTION;
END IF;
