
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefState AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefState AS source
ON target.StateKey = source.StateKey
WHEN MATCHED THEN
  UPDATE SET
  target.StateKey = source.StateKey,
 target.StateAbbr = TRIM(source.StateAbbr),
 target.StateName = TRIM(source.StateName),
 target.StateC = TRIM(source.StateC),
 target.SourceAPrimaryKey = TRIM(source.SourceAPrimaryKey),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (StateKey, StateAbbr, StateName, StateC, SourceAPrimaryKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.StateKey, TRIM(source.StateAbbr), TRIM(source.StateName), TRIM(source.StateC), TRIM(source.SourceAPrimaryKey), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT StateKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefState
      GROUP BY StateKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefState');
ELSE
  COMMIT TRANSACTION;
END IF;
