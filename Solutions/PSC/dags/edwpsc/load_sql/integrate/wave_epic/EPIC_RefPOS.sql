
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefPOS AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefPOS AS source
ON target.POSKey = source.POSKey
WHEN MATCHED THEN
  UPDATE SET
  target.POSKey = source.POSKey,
 target.POSName = TRIM(source.POSName),
 target.POSAbbr = TRIM(source.POSAbbr),
 target.POSGlPrefix = TRIM(source.POSGlPrefix),
 target.POSServAreaId = TRIM(source.POSServAreaId),
 target.ServiceAreaKey = source.ServiceAreaKey,
 target.POSAddr1 = TRIM(source.POSAddr1),
 target.POSAddr2 = TRIM(source.POSAddr2),
 target.POSCity = TRIM(source.POSCity),
 target.POSState = TRIM(source.POSState),
 target.POSZip = TRIM(source.POSZip),
 target.PosType = TRIM(source.PosType),
 target.PosCode = TRIM(source.PosCode),
 target.DeleteFlag = source.DeleteFlag,
 target.PosId = TRIM(source.PosId),
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKey = TRIM(source.SourceAPrimaryKey),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.PosTypeC = source.PosTypeC
WHEN NOT MATCHED THEN
  INSERT (POSKey, POSName, POSAbbr, POSGlPrefix, POSServAreaId, ServiceAreaKey, POSAddr1, POSAddr2, POSCity, POSState, POSZip, PosType, PosCode, DeleteFlag, PosId, RegionKey, SourceAPrimaryKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, PosTypeC)
  VALUES (source.POSKey, TRIM(source.POSName), TRIM(source.POSAbbr), TRIM(source.POSGlPrefix), TRIM(source.POSServAreaId), source.ServiceAreaKey, TRIM(source.POSAddr1), TRIM(source.POSAddr2), TRIM(source.POSCity), TRIM(source.POSState), TRIM(source.POSZip), TRIM(source.PosType), TRIM(source.PosCode), source.DeleteFlag, TRIM(source.PosId), source.RegionKey, TRIM(source.SourceAPrimaryKey), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.PosTypeC);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT POSKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefPOS
      GROUP BY POSKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefPOS');
ELSE
  COMMIT TRANSACTION;
END IF;
