
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefPosType AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefPosType AS source
ON target.PosTypeKey = source.PosTypeKey
WHEN MATCHED THEN
  UPDATE SET
  target.PosTypeKey = source.PosTypeKey,
 target.PosTypeName = TRIM(source.PosTypeName),
 target.PosTypeTitle = TRIM(source.PosTypeTitle),
 target.PosTypeAbbr = TRIM(source.PosTypeAbbr),
 target.PosTypeInternalId = TRIM(source.PosTypeInternalId),
 target.PosTypeC = TRIM(source.PosTypeC),
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKey = TRIM(source.SourceAPrimaryKey),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (PosTypeKey, PosTypeName, PosTypeTitle, PosTypeAbbr, PosTypeInternalId, PosTypeC, RegionKey, SourceAPrimaryKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.PosTypeKey, TRIM(source.PosTypeName), TRIM(source.PosTypeTitle), TRIM(source.PosTypeAbbr), TRIM(source.PosTypeInternalId), TRIM(source.PosTypeC), source.RegionKey, TRIM(source.SourceAPrimaryKey), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PosTypeKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefPosType
      GROUP BY PosTypeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefPosType');
ELSE
  COMMIT TRANSACTION;
END IF;
