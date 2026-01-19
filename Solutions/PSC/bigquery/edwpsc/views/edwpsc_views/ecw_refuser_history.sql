CREATE OR REPLACE VIEW edwpsc_views.`ecw_refuser_history`
AS SELECT
  `ecw_refuser_history`.userkey,
  `ecw_refuser_history`.userfirstname,
  `ecw_refuser_history`.userlastname,
  `ecw_refuser_history`.usermiddlename,
  `ecw_refuser_history`.username,
  `ecw_refuser_history`.userprimaryservicelocation,
  `ecw_refuser_history`.usertype,
  `ecw_refuser_history`.sourceprimarykeyvalue,
  `ecw_refuser_history`.sourcerecordlastupdated,
  `ecw_refuser_history`.dwlastupdatedatetime,
  `ecw_refuser_history`.sourcesystemcode,
  `ecw_refuser_history`.insertedby,
  `ecw_refuser_history`.inserteddtm,
  `ecw_refuser_history`.modifiedby,
  `ecw_refuser_history`.modifieddtm,
  `ecw_refuser_history`.deleteflag,
  `ecw_refuser_history`.regionkey,
  `ecw_refuser_history`.sysstarttime,
  `ecw_refuser_history`.sysendtime
  FROM
    edwpsc_base_views.`ecw_refuser_history`
;