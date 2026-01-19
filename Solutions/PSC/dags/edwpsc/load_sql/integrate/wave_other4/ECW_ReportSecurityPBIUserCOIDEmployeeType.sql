TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_ReportSecurityPBIUserCOIDEmployeeType ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_ReportSecurityPBIUserCOIDEmployeeType
(SecureEmployeeTypeAsUser34, Coid_ET, DWLastUpdateDateTime)
SELECT TRIM(source.SecureEmployeeTypeAsUser34), TRIM(source.Coid_ET), source.DWLastUpdateDateTime
FROM {{ params.param_psc_stage_dataset_name }}.ECW_ReportSecurityPBIUserCOIDEmployeeType as source;

