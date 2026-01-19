CREATE OR REPLACE VIEW edwpsc_base_views.`ccu_refnoncustomernpiconfig_history`
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
    edwpsc.`ccu_refnoncustomernpiconfig_history`
;