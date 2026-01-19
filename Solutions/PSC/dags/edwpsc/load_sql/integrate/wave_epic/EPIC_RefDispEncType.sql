
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefDispEncType AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefDispEncType AS source
ON target.DispEncTypeKey = source.DispEncTypeKey
WHEN MATCHED THEN
  UPDATE SET
  target.DispEncTypeKey = source.DispEncTypeKey,
 target.DispEncTypeName = TRIM(source.DispEncTypeName),
 target.DispEncTypeTitle = TRIM(source.DispEncTypeTitle),
 target.DispEncTypeAbbr = TRIM(source.DispEncTypeAbbr),
 target.DispEncTypeInternalId = TRIM(source.DispEncTypeInternalId),
 target.DispEncTypeC = TRIM(source.DispEncTypeC),
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKey = TRIM(source.SourceAPrimaryKey),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (DispEncTypeKey, DispEncTypeName, DispEncTypeTitle, DispEncTypeAbbr, DispEncTypeInternalId, DispEncTypeC, RegionKey, SourceAPrimaryKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.DispEncTypeKey, TRIM(source.DispEncTypeName), TRIM(source.DispEncTypeTitle), TRIM(source.DispEncTypeAbbr), TRIM(source.DispEncTypeInternalId), TRIM(source.DispEncTypeC), source.RegionKey, TRIM(source.SourceAPrimaryKey), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT DispEncTypeKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefDispEncType
      GROUP BY DispEncTypeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefDispEncType');
ELSE
  COMMIT TRANSACTION;
END IF;
