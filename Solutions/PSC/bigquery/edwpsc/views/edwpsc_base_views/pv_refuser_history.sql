CREATE OR REPLACE VIEW edwpsc_base_views.`pv_refuser_history`
AS SELECT
  `pv_refuser_history`.userkey,
  `pv_refuser_history`.userfirstname,
  `pv_refuser_history`.userlastname,
  `pv_refuser_history`.usermiddlename,
  `pv_refuser_history`.username,
  `pv_refuser_history`.usertype,
  `pv_refuser_history`.sourceprimarykeyvalue,
  `pv_refuser_history`.userprofilepk,
  `pv_refuser_history`.userstatus,
  `pv_refuser_history`.sourcerecordlastupdated,
  `pv_refuser_history`.dwlastupdatedatetime,
  `pv_refuser_history`.sourcesystemcode,
  `pv_refuser_history`.insertedby,
  `pv_refuser_history`.inserteddtm,
  `pv_refuser_history`.modifiedby,
  `pv_refuser_history`.modifieddtm,
  `pv_refuser_history`.deleteflag,
  `pv_refuser_history`.regionkey,
  `pv_refuser_history`.sysstarttime,
  `pv_refuser_history`.sysendtime
  FROM
    edwpsc.`pv_refuser_history`
;