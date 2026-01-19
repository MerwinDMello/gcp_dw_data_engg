
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefInvStatus AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefInvStatus AS source
ON target.InvStatusKey = source.InvStatusKey
WHEN MATCHED THEN
  UPDATE SET
  target.InvStatusKey = source.InvStatusKey,
 target.InvStatusName = TRIM(source.InvStatusName),
 target.InvStatusTitle = TRIM(source.InvStatusTitle),
 target.InvStatusAbbr = TRIM(source.InvStatusAbbr),
 target.InvStatusInternalId = TRIM(source.InvStatusInternalId),
 target.InvStatusC = TRIM(source.InvStatusC),
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKey = TRIM(source.SourceAPrimaryKey),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (InvStatusKey, InvStatusName, InvStatusTitle, InvStatusAbbr, InvStatusInternalId, InvStatusC, RegionKey, SourceAPrimaryKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.InvStatusKey, TRIM(source.InvStatusName), TRIM(source.InvStatusTitle), TRIM(source.InvStatusAbbr), TRIM(source.InvStatusInternalId), TRIM(source.InvStatusC), source.RegionKey, TRIM(source.SourceAPrimaryKey), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT InvStatusKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefInvStatus
      GROUP BY InvStatusKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefInvStatus');
ELSE
  COMMIT TRANSACTION;
END IF;
