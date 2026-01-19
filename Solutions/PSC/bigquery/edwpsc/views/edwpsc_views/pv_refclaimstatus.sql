CREATE OR REPLACE VIEW edwpsc_views.`pv_refclaimstatus`
AS SELECT
  `pv_refclaimstatus`.claimstatuskey,
  `pv_refclaimstatus`.claimstatusname,
  `pv_refclaimstatus`.claimstatusshortname,
  `pv_refclaimstatus`.sourceprimarykeyvalue,
  `pv_refclaimstatus`.sourcerecordlastupdated,
  `pv_refclaimstatus`.dwlastupdatedatetime,
  `pv_refclaimstatus`.sourcesystemcode,
  `pv_refclaimstatus`.insertedby,
  `pv_refclaimstatus`.inserteddtm,
  `pv_refclaimstatus`.modifiedby,
  `pv_refclaimstatus`.modifieddtm,
  `pv_refclaimstatus`.deleteflag,
  `pv_refclaimstatus`.claimstatusfulldesc
  FROM
    edwpsc_base_views.`pv_refclaimstatus`
;