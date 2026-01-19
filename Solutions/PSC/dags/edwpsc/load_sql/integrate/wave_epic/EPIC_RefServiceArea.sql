
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefServiceArea AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefServiceArea AS source
ON target.ServiceAreaKey = source.ServiceAreaKey
WHEN MATCHED THEN
  UPDATE SET
  target.ServiceAreaKey = source.ServiceAreaKey,
 target.ServiceAreaName = TRIM(source.ServiceAreaName),
 target.ServiceAreaAbbr = TRIM(source.ServiceAreaAbbr),
 target.ServiceAreaType = TRIM(source.ServiceAreaType),
 target.ServiceAreaGroup = TRIM(source.ServiceAreaGroup),
 target.ServiceAreaGlPrefix = TRIM(source.ServiceAreaGlPrefix),
 target.ServAreaId = TRIM(source.ServAreaId),
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKey = TRIM(source.SourceAPrimaryKey),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ServiceAreaKey, ServiceAreaName, ServiceAreaAbbr, ServiceAreaType, ServiceAreaGroup, ServiceAreaGlPrefix, ServAreaId, RegionKey, SourceAPrimaryKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ServiceAreaKey, TRIM(source.ServiceAreaName), TRIM(source.ServiceAreaAbbr), TRIM(source.ServiceAreaType), TRIM(source.ServiceAreaGroup), TRIM(source.ServiceAreaGlPrefix), TRIM(source.ServAreaId), source.RegionKey, TRIM(source.SourceAPrimaryKey), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ServiceAreaKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefServiceArea
      GROUP BY ServiceAreaKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefServiceArea');
ELSE
  COMMIT TRANSACTION;
END IF;
