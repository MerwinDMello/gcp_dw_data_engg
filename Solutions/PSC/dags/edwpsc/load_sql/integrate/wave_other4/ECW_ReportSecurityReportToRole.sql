
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_ReportSecurityReportToRole ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_ReportSecurityReportToRole (ReportRoleID, RoleID, ReportID)
SELECT source.ReportRoleID, source.RoleID, source.ReportID
FROM {{ params.param_psc_stage_dataset_name }}.ECW_ReportSecurityReportToRole as source;
