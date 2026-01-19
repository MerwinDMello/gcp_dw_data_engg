TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_ReportSecurityPBIUser ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_ReportSecurityPBIUser
(FullUser34, User34, UserPBI, EmployeeType, RoleID, SecureCOIDAsUser34, SecureEmployeeTypeAsUser34, SecureAllAsOne, DWLastUpdateDateTime)
SELECT TRIM(source.FullUser34), TRIM(source.User34), TRIM(source.UserPBI), TRIM(source.EmployeeType), source.RoleID, TRIM(source.SecureCOIDAsUser34), TRIM(source.SecureEmployeeTypeAsUser34), source.SecureAllAsOne, source.DWLastUpdateDateTime
FROM {{ params.param_psc_stage_dataset_name }}.ECW_ReportSecurityPBIUser as source;
