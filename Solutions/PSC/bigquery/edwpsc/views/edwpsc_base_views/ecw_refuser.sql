CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refuser`
AS SELECT
  `ecw_refuser`.userkey,
  `ecw_refuser`.userfirstname,
  `ecw_refuser`.userlastname,
  `ecw_refuser`.usermiddlename,
  `ecw_refuser`.username,
  `ecw_refuser`.userprimaryservicelocation,
  `ecw_refuser`.usertype,
  `ecw_refuser`.sourceprimarykeyvalue,
  `ecw_refuser`.sourcerecordlastupdated,
  `ecw_refuser`.dwlastupdatedatetime,
  `ecw_refuser`.sourcesystemcode,
  `ecw_refuser`.insertedby,
  `ecw_refuser`.inserteddtm,
  `ecw_refuser`.modifiedby,
  `ecw_refuser`.modifieddtm,
  `ecw_refuser`.deleteflag,
  `ecw_refuser`.regionkey,
  `ecw_refuser`.sysstarttime,
  `ecw_refuser`.sysendtime
  FROM
    edwpsc.`ecw_refuser`
;