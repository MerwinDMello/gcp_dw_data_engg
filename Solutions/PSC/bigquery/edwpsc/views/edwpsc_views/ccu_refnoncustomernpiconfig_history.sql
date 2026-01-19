CREATE OR REPLACE VIEW edwpsc_views.`ccu_refnoncustomernpiconfig_history`
AS SELECT
  `ccu_refnoncustomernpiconfig_history`.ccunoncustomernpiconfigkey,
  `ccu_refnoncustomernpiconfig_history`.ccunoncustomerconfigkey,
  `ccu_refnoncustomernpiconfig_history`.providernpi,
  `ccu_refnoncustomernpiconfig_history`.executionorder,
  `ccu_refnoncustomernpiconfig_history`.effectivedatekey,
  `ccu_refnoncustomernpiconfig_history`.terminationdatekey,
  `ccu_refnoncustomernpiconfig_history`.dwlastupdatedatetime,
  `ccu_refnoncustomernpiconfig_history`.insertedby,
  `ccu_refnoncustomernpiconfig_history`.inserteddtm,
  `ccu_refnoncustomernpiconfig_history`.modifiedby,
  `ccu_refnoncustomernpiconfig_history`.modifieddtm,
  `ccu_refnoncustomernpiconfig_history`.approvedflag,
  `ccu_refnoncustomernpiconfig_history`.approveddate,
  `ccu_refnoncustomernpiconfig_history`.approvedbyid,
  `ccu_refnoncustomernpiconfig_history`.sysstarttime,
  `ccu_refnoncustomernpiconfig_history`.sysendtime,
  `ccu_refnoncustomernpiconfig_history`.coid
  FROM
    edwpsc_base_views.`ccu_refnoncustomernpiconfig_history`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ccu_refnoncustomernpiconfig_history`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;