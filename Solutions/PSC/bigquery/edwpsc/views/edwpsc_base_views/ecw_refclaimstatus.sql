CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refclaimstatus`
AS SELECT
  `ecw_refclaimstatus`.claimstatuskey,
  `ecw_refclaimstatus`.claimstatusname,
  `ecw_refclaimstatus`.claimstatusshortname,
  `ecw_refclaimstatus`.sourceprimarykeyvalue,
  `ecw_refclaimstatus`.sourcerecordlastupdated,
  `ecw_refclaimstatus`.dwlastupdatedatetime,
  `ecw_refclaimstatus`.sourcesystemcode,
  `ecw_refclaimstatus`.insertedby,
  `ecw_refclaimstatus`.inserteddtm,
  `ecw_refclaimstatus`.modifiedby,
  `ecw_refclaimstatus`.modifieddtm,
  `ecw_refclaimstatus`.deleteflag,
  `ecw_refclaimstatus`.sysstarttime,
  `ecw_refclaimstatus`.sysendtime
  FROM
    edwpsc.`ecw_refclaimstatus`
;