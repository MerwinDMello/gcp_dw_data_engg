
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefApptStatus AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefApptStatus AS source
ON target.ApptStatusKey = source.ApptStatusKey
WHEN MATCHED THEN
  UPDATE SET
  target.ApptStatusKey = source.ApptStatusKey,
 target.ApptStatusName = TRIM(source.ApptStatusName),
 target.ApptStatusTitle = TRIM(source.ApptStatusTitle),
 target.ApptStatusAbbr = TRIM(source.ApptStatusAbbr),
 target.ApptStatusInternalId = TRIM(source.ApptStatusInternalId),
 target.ApptStatusC = TRIM(source.ApptStatusC),
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKey = TRIM(source.SourceAPrimaryKey),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ApptStatusKey, ApptStatusName, ApptStatusTitle, ApptStatusAbbr, ApptStatusInternalId, ApptStatusC, RegionKey, SourceAPrimaryKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ApptStatusKey, TRIM(source.ApptStatusName), TRIM(source.ApptStatusTitle), TRIM(source.ApptStatusAbbr), TRIM(source.ApptStatusInternalId), TRIM(source.ApptStatusC), source.RegionKey, TRIM(source.SourceAPrimaryKey), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ApptStatusKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefApptStatus
      GROUP BY ApptStatusKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefApptStatus');
ELSE
  COMMIT TRANSACTION;
END IF;
