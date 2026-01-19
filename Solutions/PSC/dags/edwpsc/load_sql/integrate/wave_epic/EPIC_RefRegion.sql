
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.EPIC_RefRegion ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefRegion (RegionKey, RegionName, RegionSourceConnectionString, RegionSourceActive, RunOrder, RegionDBName, RegionServerName, RegionSSRSStagePackage, RegionLastRunStageCompleteFlag, RegionLastRunStageFailureMessage, RegionRedirectLogPath, TimeZoneDifference)
SELECT source.RegionKey, TRIM(source.RegionName), TRIM(source.RegionSourceConnectionString), source.RegionSourceActive, source.RunOrder, TRIM(source.RegionDBName), TRIM(source.RegionServerName), TRIM(source.RegionSSRSStagePackage), source.RegionLastRunStageCompleteFlag, TRIM(source.RegionLastRunStageFailureMessage), TRIM(source.RegionRedirectLogPath), source.TimeZoneDifference
FROM {{ params.param_psc_stage_dataset_name }}.EPIC_RefRegion as source;
