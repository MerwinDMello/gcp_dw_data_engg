CREATE OR REPLACE VIEW edwpsc_views.`ecw_refclaimstatus_history`
AS SELECT
  `ecw_refclaimstatus_history`.claimstatuskey,
  `ecw_refclaimstatus_history`.claimstatusname,
  `ecw_refclaimstatus_history`.claimstatusshortname,
  `ecw_refclaimstatus_history`.sourceprimarykeyvalue,
  `ecw_refclaimstatus_history`.sourcerecordlastupdated,
  `ecw_refclaimstatus_history`.dwlastupdatedatetime,
  `ecw_refclaimstatus_history`.sourcesystemcode,
  `ecw_refclaimstatus_history`.insertedby,
  `ecw_refclaimstatus_history`.inserteddtm,
  `ecw_refclaimstatus_history`.modifiedby,
  `ecw_refclaimstatus_history`.modifieddtm,
  `ecw_refclaimstatus_history`.deleteflag,
  `ecw_refclaimstatus_history`.sysstarttime,
  `ecw_refclaimstatus_history`.sysendtime
  FROM
    edwpsc_base_views.`ecw_refclaimstatus_history`
;