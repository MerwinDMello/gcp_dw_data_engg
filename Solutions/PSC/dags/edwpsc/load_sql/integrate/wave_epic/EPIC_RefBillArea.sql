
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefBillArea AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefBillArea AS source
ON target.BillAreaKey = source.BillAreaKey
WHEN MATCHED THEN
  UPDATE SET
  target.BillAreaKey = source.BillAreaKey,
 target.BillAreaName = TRIM(source.BillAreaName),
 target.BillAreaAbbr = TRIM(source.BillAreaAbbr),
 target.BillAreaGlPrefix = TRIM(source.BillAreaGlPrefix),
 target.BillAreaId = TRIM(source.BillAreaId),
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKey = TRIM(source.SourceAPrimaryKey),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.BillAreaFederalTaxId = TRIM(source.BillAreaFederalTaxId)
WHEN NOT MATCHED THEN
  INSERT (BillAreaKey, BillAreaName, BillAreaAbbr, BillAreaGlPrefix, BillAreaId, RegionKey, SourceAPrimaryKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, BillAreaFederalTaxId)
  VALUES (source.BillAreaKey, TRIM(source.BillAreaName), TRIM(source.BillAreaAbbr), TRIM(source.BillAreaGlPrefix), TRIM(source.BillAreaId), source.RegionKey, TRIM(source.SourceAPrimaryKey), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.BillAreaFederalTaxId));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT BillAreaKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefBillArea
      GROUP BY BillAreaKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefBillArea');
ELSE
  COMMIT TRANSACTION;
END IF;
