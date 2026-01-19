
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactEncounterCPT AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactEncounterCPT AS source
ON target.EncounterCPTKey = source.EncounterCPTKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncounterCPTKey = source.EncounterCPTKey,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.PracticeName = TRIM(source.PracticeName),
 target.EncounterKey = source.EncounterKey,
 target.EncounterID = source.EncounterID,
 target.CPTCodeKey = source.CPTCodeKey,
 target.CPTCode = TRIM(source.CPTCode),
 target.CPTUnits = source.CPTUnits,
 target.VisitDate = source.VisitDate,
 target.PrimaryFlag = source.PrimaryFlag,
 target.DeleteFlag = source.DeleteFlag,
 target.SourcePrimaryKeyValue = TRIM(source.SourcePrimaryKeyValue),
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (EncounterCPTKey, RegionKey, Coid, PracticeName, EncounterKey, EncounterID, CPTCodeKey, CPTCode, CPTUnits, VisitDate, PrimaryFlag, DeleteFlag, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.EncounterCPTKey, source.RegionKey, TRIM(source.Coid), TRIM(source.PracticeName), source.EncounterKey, source.EncounterID, source.CPTCodeKey, TRIM(source.CPTCode), source.CPTUnits, source.VisitDate, source.PrimaryFlag, source.DeleteFlag, TRIM(source.SourcePrimaryKeyValue), source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterCPTKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactEncounterCPT
      GROUP BY EncounterCPTKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactEncounterCPT');
ELSE
  COMMIT TRANSACTION;
END IF;
