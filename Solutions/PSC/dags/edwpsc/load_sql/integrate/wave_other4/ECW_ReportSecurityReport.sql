
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_ReportSecurityReport ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_ReportSecurityReport (ReportID, ReportName)
SELECT source.ReportID, TRIM(source.ReportName)
FROM {{ params.param_psc_stage_dataset_name }}.ECW_ReportSecurityReport as source;
