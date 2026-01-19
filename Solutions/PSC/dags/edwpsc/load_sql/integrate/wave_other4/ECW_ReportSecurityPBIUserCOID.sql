TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_ReportSecurityPBIUserCOID ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_ReportSecurityPBIUserCOID
(SecureCOIDAsUser34, Coid, DWLastUpdateDateTime)
SELECT TRIM(source.SecureCOIDAsUser34), TRIM(source.Coid), source.DWLastUpdateDateTime
FROM {{ params.param_psc_stage_dataset_name }}.ECW_ReportSecurityPBIUserCOID as source;

