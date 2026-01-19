
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefRegionMeditechExpanse ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefRegionMeditechExpanse (RegionKey, RegionName, RegionSourceConnectionString, RegionSourceActive, RunOrder, RegionDBName, RegionServerName, RegionSSRSStagePackage, RegionLastRunStageCompleteFlag, RegionLastRunStageFailureMessage, RegionRedirectLogPath)
SELECT source.RegionKey, TRIM(source.RegionName), TRIM(source.RegionSourceConnectionString), source.RegionSourceActive, source.RunOrder, TRIM(source.RegionDBName), TRIM(source.RegionServerName), TRIM(source.RegionSSRSStagePackage), source.RegionLastRunStageCompleteFlag, TRIM(source.RegionLastRunStageFailureMessage), TRIM(source.RegionRedirectLogPath)
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefRegionMeditechExpanse as source;
