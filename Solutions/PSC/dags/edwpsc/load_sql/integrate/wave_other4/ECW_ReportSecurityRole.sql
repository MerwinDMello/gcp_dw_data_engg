
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_ReportSecurityRole ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_ReportSecurityRole (RoleID, RoleName)
SELECT source.RoleID, TRIM(source.RoleName)
FROM {{ params.param_psc_stage_dataset_name }}.ECW_ReportSecurityRole as source;
