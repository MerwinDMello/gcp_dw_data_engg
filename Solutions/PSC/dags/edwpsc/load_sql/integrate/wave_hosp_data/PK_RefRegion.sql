
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PK_RefRegion AS target
USING {{ params.param_psc_stage_dataset_name }}.PK_RefRegion AS source
ON target.PKRegionKey = source.PKRegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.PKRegionKey = source.PKRegionKey,
 target.PKRegionName = TRIM(source.PKRegionName),
 target.PKServerReferenceName = TRIM(source.PKServerReferenceName),
 target.PKRegionActiveFlag = source.PKRegionActiveFlag,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.RegionName = TRIM(source.RegionName),
 target.RegionSourceConnectionString = TRIM(source.RegionSourceConnectionString),
 target.RegionSourceActive = source.RegionSourceActive,
 target.RegionDBName = TRIM(source.RegionDBName),
 target.RegionServerName = TRIM(source.RegionServerName),
 target.RegionSSRSStagePackage = TRIM(source.RegionSSRSStagePackage),
 target.RegionLastRunStageCompleteFlag = source.RegionLastRunStageCompleteFlag,
 target.RegionLastRunStageFailureMessage = TRIM(source.RegionLastRunStageFailureMessage),
 target.RegionRedirectLogPath = TRIM(source.RegionRedirectLogPath),
 target.SchemaName = TRIM(source.SchemaName),
 target.EnvironmentId = TRIM(source.EnvironmentId),
 target.MiscRunFlag = source.MiscRunFlag,
 target.TimeZoneDifference = source.TimeZoneDifference,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (PKRegionKey, PKRegionName, PKServerReferenceName, PKRegionActiveFlag, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, RegionName, RegionSourceConnectionString, RegionSourceActive, RegionDBName, RegionServerName, RegionSSRSStagePackage, RegionLastRunStageCompleteFlag, RegionLastRunStageFailureMessage, RegionRedirectLogPath, SchemaName, EnvironmentId, MiscRunFlag, TimeZoneDifference, DWLastUpdateDateTime)
  VALUES (source.PKRegionKey, TRIM(source.PKRegionName), TRIM(source.PKServerReferenceName), source.PKRegionActiveFlag, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.RegionName), TRIM(source.RegionSourceConnectionString), source.RegionSourceActive, TRIM(source.RegionDBName), TRIM(source.RegionServerName), TRIM(source.RegionSSRSStagePackage), source.RegionLastRunStageCompleteFlag, TRIM(source.RegionLastRunStageFailureMessage), TRIM(source.RegionRedirectLogPath), TRIM(source.SchemaName), TRIM(source.EnvironmentId), source.MiscRunFlag, source.TimeZoneDifference, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PKRegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PK_RefRegion
      GROUP BY PKRegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PK_RefRegion');
ELSE
  COMMIT TRANSACTION;
END IF;
