
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefProcedureCode AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefProcedureCode AS source
ON target.ProcedureCodeKey = source.ProcedureCodeKey
WHEN MATCHED THEN
  UPDATE SET
  target.ProcedureCodeKey = source.ProcedureCodeKey,
 target.ProcedureCode = TRIM(source.ProcedureCode),
 target.ProcedureCodeName = TRIM(source.ProcedureCodeName),
 target.ProcedureCodeDescription = TRIM(source.ProcedureCodeDescription),
 target.ProcedureCodeShortName = TRIM(source.ProcedureCodeShortName),
 target.ProcedureCodeActive = source.ProcedureCodeActive,
 target.ProcId = TRIM(source.ProcId),
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKey = TRIM(source.SourceAPrimaryKey),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ProcedureCodeKey, ProcedureCode, ProcedureCodeName, ProcedureCodeDescription, ProcedureCodeShortName, ProcedureCodeActive, ProcId, RegionKey, SourceAPrimaryKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ProcedureCodeKey, TRIM(source.ProcedureCode), TRIM(source.ProcedureCodeName), TRIM(source.ProcedureCodeDescription), TRIM(source.ProcedureCodeShortName), source.ProcedureCodeActive, TRIM(source.ProcId), source.RegionKey, TRIM(source.SourceAPrimaryKey), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ProcedureCodeKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefProcedureCode
      GROUP BY ProcedureCodeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefProcedureCode');
ELSE
  COMMIT TRANSACTION;
END IF;
