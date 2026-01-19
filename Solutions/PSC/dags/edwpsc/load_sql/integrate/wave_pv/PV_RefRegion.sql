
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.PV_RefRegion ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.PV_RefRegion (RegionKey, RegionName, RegionSourceConnectionString, RegionSourceActive, ArtivaRegionKey, RegionDBName, RegionServerName, RegionSSRSStagePackage, RegionLastRunStageCompleteFlag, RegionLastRunStageFailureMessage, RegionRedirectLogPath)
SELECT source.RegionKey, TRIM(source.RegionName), TRIM(source.RegionSourceConnectionString), source.RegionSourceActive, source.ArtivaRegionKey, TRIM(source.RegionDBName), TRIM(source.RegionServerName), TRIM(source.RegionSSRSStagePackage), source.RegionLastRunStageCompleteFlag, TRIM(source.RegionLastRunStageFailureMessage), TRIM(source.RegionRedirectLogPath)
FROM {{ params.param_psc_stage_dataset_name }}.PV_RefRegion as source;
