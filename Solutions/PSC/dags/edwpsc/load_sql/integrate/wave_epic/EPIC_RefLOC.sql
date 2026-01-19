
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefLOC AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefLOC AS source
ON target.LOCKey = source.LOCKey
WHEN MATCHED THEN
  UPDATE SET
  target.LOCKey = source.LOCKey,
 target.LOCName = TRIM(source.LOCName),
 target.LOCAbbr = TRIM(source.LOCAbbr),
 target.LOCGlPrefix = TRIM(source.LOCGlPrefix),
 target.LOCServAreaId = TRIM(source.LOCServAreaId),
 target.ServiceAreaKey = source.ServiceAreaKey,
 target.DeleteFlag = source.DeleteFlag,
 target.LOCPosType = TRIM(source.LOCPosType),
 target.LOCPosCode = TRIM(source.LOCPosCode),
 target.LOCFacilityId = TRIM(source.LOCFacilityId),
 target.LocId = TRIM(source.LocId),
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKey = TRIM(source.SourceAPrimaryKey),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (LOCKey, LOCName, LOCAbbr, LOCGlPrefix, LOCServAreaId, ServiceAreaKey, DeleteFlag, LOCPosType, LOCPosCode, LOCFacilityId, LocId, RegionKey, SourceAPrimaryKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.LOCKey, TRIM(source.LOCName), TRIM(source.LOCAbbr), TRIM(source.LOCGlPrefix), TRIM(source.LOCServAreaId), source.ServiceAreaKey, source.DeleteFlag, TRIM(source.LOCPosType), TRIM(source.LOCPosCode), TRIM(source.LOCFacilityId), TRIM(source.LocId), source.RegionKey, TRIM(source.SourceAPrimaryKey), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT LOCKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefLOC
      GROUP BY LOCKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefLOC');
ELSE
  COMMIT TRANSACTION;
END IF;
