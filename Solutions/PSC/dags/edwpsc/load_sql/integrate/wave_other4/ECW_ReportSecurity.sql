TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_ReportSecurity;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_ReportSecurity
  (
    User34,
    SecurityCOID,
    SecurityRole,
    MstrRole,
    MstrAltCoid,
    MstrCoid,
    MstrMarket,
    MstrDivision,
    MstrGroup,
    MstrFullCoid,
    DashboardSecurityCOID,
    DashboardSecurityRole,
    MissingCOIDsFromMSTR,
    SSRSOnlyFlag,
    SSRSLastAccessed,
    DWLastUpdateDateTime)
SELECT
  TRIM(source.User34),
  TRIM(source.SecurityCOID),
  TRIM(source.SecurityRole),
  TRIM(source.MstrRole),
  TRIM(source.MstrAltCoid),
  TRIM(source.MstrCoid),
  TRIM(source.MstrMarket),
  TRIM(source.MstrDivision),
  TRIM(source.MstrGroup),
  TRIM(source.MstrFullCoid),
  TRIM(source.DashboardSecurityCOID),
  TRIM(source.DashboardSecurityRole),
  TRIM(source.MissingCOIDsFromMSTR),
  source.SSRSOnlyFlag,
  source.SSRSLastAccessed,
  source.DWLastUpdateDateTime
FROM {{ params.param_psc_stage_dataset_name }}.ECW_ReportSecurity AS source;
