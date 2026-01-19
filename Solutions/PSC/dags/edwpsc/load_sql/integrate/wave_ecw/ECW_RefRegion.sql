
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefRegion ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefRegion (RegionKey, RegionName, RegionSystemName, RegionDBSnapshotName, RegionDBLiveName, RegionDBCDCName, RegionActive, LastETLUpdate, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, RegionServerName, RegionSSRSStagePackage, RegionLastRunStageCompleteFlag, RegionLastRunStageFailureMessage, RegionRedirectLogPath, RegionPrefix, TimeZoneDifference, ValescoIndicator, AccountPrefix)
SELECT source.RegionKey, TRIM(source.RegionName), TRIM(source.RegionSystemName), TRIM(source.RegionDBSnapshotName), TRIM(source.RegionDBLiveName), TRIM(source.RegionDBCDCName), source.RegionActive, source.LastETLUpdate, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.RegionServerName), TRIM(source.RegionSSRSStagePackage), source.RegionLastRunStageCompleteFlag, TRIM(source.RegionLastRunStageFailureMessage), TRIM(source.RegionRedirectLogPath), TRIM(source.RegionPrefix), source.TimeZoneDifference, TRIM(source.ValescoIndicator), TRIM(source.AccountPrefix)
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefRegion as source;
