
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_ReportSecurityMasterData ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_ReportSecurityMasterData (user34, MasterDataControl)
SELECT TRIM(source.user34), TRIM(source.MasterDataControl)
FROM {{ params.param_psc_stage_dataset_name }}.ECW_ReportSecurityMasterData as source;
