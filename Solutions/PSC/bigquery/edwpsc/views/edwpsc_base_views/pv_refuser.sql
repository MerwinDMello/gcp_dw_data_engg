CREATE OR REPLACE VIEW edwpsc_base_views.`pv_refuser`
AS SELECT
  `pv_refuser`.userkey,
  `pv_refuser`.userfirstname,
  `pv_refuser`.userlastname,
  `pv_refuser`.usermiddlename,
  `pv_refuser`.username,
  `pv_refuser`.usertype,
  `pv_refuser`.sourceprimarykeyvalue,
  `pv_refuser`.userprofilepk,
  `pv_refuser`.userstatus,
  `pv_refuser`.sourcerecordlastupdated,
  `pv_refuser`.dwlastupdatedatetime,
  `pv_refuser`.sourcesystemcode,
  `pv_refuser`.insertedby,
  `pv_refuser`.inserteddtm,
  `pv_refuser`.modifiedby,
  `pv_refuser`.modifieddtm,
  `pv_refuser`.deleteflag,
  `pv_refuser`.regionkey,
  `pv_refuser`.sysstarttime,
  `pv_refuser`.sysendtime
  FROM
    edwpsc.`pv_refuser`
;