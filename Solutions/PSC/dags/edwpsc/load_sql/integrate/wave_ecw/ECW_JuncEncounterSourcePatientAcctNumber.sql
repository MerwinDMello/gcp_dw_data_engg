
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncEncounterSourcePatientAcctNumber AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_JuncEncounterSourcePatientAcctNumber AS source
ON target.EncSourcePatAcctNumberKey = source.EncSourcePatAcctNumberKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncSourcePatAcctNumberKey = source.EncSourcePatAcctNumberKey,
 target.RegionKey = source.RegionKey,
 target.EncounterID = source.EncounterID,
 target.SourcePatientAcctNumber = TRIM(source.SourcePatientAcctNumber),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (EncSourcePatAcctNumberKey, RegionKey, EncounterID, SourcePatientAcctNumber, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.EncSourcePatAcctNumberKey, source.RegionKey, source.EncounterID, TRIM(source.SourcePatientAcctNumber), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncSourcePatAcctNumberKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_JuncEncounterSourcePatientAcctNumber
      GROUP BY EncSourcePatAcctNumberKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_JuncEncounterSourcePatientAcctNumber');
ELSE
  COMMIT TRANSACTION;
END IF;
