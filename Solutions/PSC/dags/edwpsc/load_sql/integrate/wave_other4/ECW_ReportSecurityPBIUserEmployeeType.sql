
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_ReportSecurityPBIUserEmployeeType ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_ReportSecurityPBIUserEmployeeType (UserEmployeeType, EmployeeType)
SELECT TRIM(source.UserEmployeeType), TRIM(source.EmployeeType)
FROM {{ params.param_psc_stage_dataset_name }}.ECW_ReportSecurityPBIUserEmployeeType as source;
