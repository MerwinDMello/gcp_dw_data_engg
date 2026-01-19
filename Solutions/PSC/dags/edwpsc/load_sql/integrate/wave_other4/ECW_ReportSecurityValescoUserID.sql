
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_ReportSecurityValescoUserID ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_ReportSecurityValescoUserID (user34, RoleID, UserTypeName, SecurityCOID)
SELECT TRIM(source.user34), source.RoleID, TRIM(source.UserTypeName), TRIM(source.SecurityCOID)
FROM {{ params.param_psc_stage_dataset_name }}.ECW_ReportSecurityValescoUserID as source;
