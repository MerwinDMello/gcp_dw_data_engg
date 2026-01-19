
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_ReportSecurityUserCOID;


INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_ReportSecurityUserCOID
  (FullUser34, UserDomain, User34, UserPBI, coid, DWLastUpdateDateTime)
SELECT
  TRIM(source.FullUser34),
  TRIM(source.UserDomain),
  TRIM(source.User34),
  TRIM(source.UserPBI),
  TRIM(source.coid),
  source.DWLastUpdateDateTime
FROM
  {{ params.param_psc_stage_dataset_name }}.ECW_ReportSecurityUserCOID
    AS source;
