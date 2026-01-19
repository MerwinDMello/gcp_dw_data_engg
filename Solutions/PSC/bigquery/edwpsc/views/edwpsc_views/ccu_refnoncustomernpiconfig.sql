CREATE OR REPLACE VIEW edwpsc_views.`ccu_refnoncustomernpiconfig`
AS SELECT
  `ccu_refnoncustomernpiconfig`.ccunoncustomernpiconfigkey,
  `ccu_refnoncustomernpiconfig`.ccunoncustomerconfigkey,
  `ccu_refnoncustomernpiconfig`.providernpi,
  `ccu_refnoncustomernpiconfig`.executionorder,
  `ccu_refnoncustomernpiconfig`.effectivedatekey,
  `ccu_refnoncustomernpiconfig`.terminationdatekey,
  `ccu_refnoncustomernpiconfig`.dwlastupdatedatetime,
  `ccu_refnoncustomernpiconfig`.insertedby,
  `ccu_refnoncustomernpiconfig`.inserteddtm,
  `ccu_refnoncustomernpiconfig`.modifiedby,
  `ccu_refnoncustomernpiconfig`.modifieddtm,
  `ccu_refnoncustomernpiconfig`.approvedflag,
  `ccu_refnoncustomernpiconfig`.approveddate,
  `ccu_refnoncustomernpiconfig`.approvedbyid,
  `ccu_refnoncustomernpiconfig`.sysstarttime,
  `ccu_refnoncustomernpiconfig`.sysendtime,
  `ccu_refnoncustomernpiconfig`.coid
  FROM
    edwpsc_base_views.`ccu_refnoncustomernpiconfig`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ccu_refnoncustomernpiconfig`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;