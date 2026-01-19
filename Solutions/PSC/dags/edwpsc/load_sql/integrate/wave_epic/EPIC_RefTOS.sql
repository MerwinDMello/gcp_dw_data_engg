
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefTOS AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefTOS AS source
ON target.TosKey = source.TosKey
WHEN MATCHED THEN
  UPDATE SET
  target.TosKey = source.TosKey,
 target.TosName = TRIM(source.TosName),
 target.TosTitle = TRIM(source.TosTitle),
 target.TosAbbr = TRIM(source.TosAbbr),
 target.TosInternalId = TRIM(source.TosInternalId),
 target.TosC = TRIM(source.TosC),
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKey = TRIM(source.SourceAPrimaryKey),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (TosKey, TosName, TosTitle, TosAbbr, TosInternalId, TosC, RegionKey, SourceAPrimaryKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.TosKey, TRIM(source.TosName), TRIM(source.TosTitle), TRIM(source.TosAbbr), TRIM(source.TosInternalId), TRIM(source.TosC), source.RegionKey, TRIM(source.SourceAPrimaryKey), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT TosKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefTOS
      GROUP BY TosKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefTOS');
ELSE
  COMMIT TRANSACTION;
END IF;
